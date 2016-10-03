/*
 * RingBuffer is a fixed-length-element and contiguous-block circular persistent buffer.
 *
 * Copyright (C) 2016  Andreas Lüthi
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package ch.luethi.ringbuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import static java.util.Arrays.copyOfRange;

public class RingBuffer {

    public static final int DEFAULT_REC_LEN = 32;
    public String dataFile;
    private long capacity;
    private long count;
    private long last;

    private static final byte HEADER_LEN = 20;
    private int recLen = DEFAULT_REC_LEN;
    private RandomAccessFile raf;


    public RingBuffer(String dataFile, long initCapacity, int recLen) {
        this.dataFile = dataFile;
        capacity = initCapacity;
        try {
            raf = new RandomAccessFile(this.dataFile, "rw");
            setCapacity(capacity);
            count = 0;
            last = 0;
            this.recLen = recLen;
            writeRecLen(recLen);
            updateHeader();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RingBuffer(String dataFile) {
        this.dataFile = dataFile;
        try {
            boolean rafExits = (new File(this.dataFile)).exists();
            raf = new RandomAccessFile(this.dataFile, "rw");
            capacity = (raf.length() - HEADER_LEN) / recLen;
            if (rafExits) {
                readRecLen();
                readHeader();
            } else {
                count = 0;
                last = 0;
                recLen = DEFAULT_REC_LEN;
                writeRecLen(recLen);
                updateHeader();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void setCapacity(long capacity) {
        try {
            this.capacity = capacity;
            raf.setLength((capacity * recLen) + HEADER_LEN);
            updateHeader();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getRecLen() {
        return recLen;
    }

    public void setRecLen(int recLen) {
        this.recLen = recLen;
    }

    public void push(byte[] data) {
        if (data.length != recLen) {
            throw new RuntimeException("Date length error, length must be " + recLen);
        }
        if (this.capacity == 0) {
            throw new RuntimeException("Storage capacity is 0");
        }
        try {
            count = Math.min(count + 1, capacity);
            last = (last + 1) % capacity;
            raf.seek(HEADER_LEN + (last * recLen));
            raf.write(data);
            updateHeader();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] pop() {
        if (count > 0) {
            try {
                raf.seek(HEADER_LEN + (last * recLen));
                byte[] ba = new byte[recLen];
                raf.read(ba);
                count = count - 1;
                last = (last == 0 ? capacity : last) - 1;
                updateHeader();
                return ba;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public byte[] peek() {
        if (count > 0) {
            try {
                raf.seek(HEADER_LEN + (last * recLen));
                byte[] ba = new byte[recLen];
                raf.read(ba);
                return ba;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public byte[][] peek(int num) {
        ArrayList<byte[]> list = new ArrayList<byte[]>(num);
        long tlast = last;
        long mnum = Math.min(count, num);

        for (int i = 0; i < mnum; i++) {
            try {
                raf.seek(HEADER_LEN + (tlast * recLen));
                byte[] ba = new byte[recLen];
                raf.read(ba);
                list.add(ba);
                tlast = (tlast == 0 ? capacity : tlast) - 1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        byte[][] ret = new byte[list.size()][recLen];
        return list.toArray(ret);
    }

    // todo -- optimize with: public static byte[] copyOfRange(byte[] original, int from, int to)
    public byte[][] peek2(int num) {
        long tlast = last;
        int tnum = num;
        byte[] bax = new byte[0];
        long tlast2 = 0;
        int tnum2 = 0;
        byte[] bax2 = new byte[0];

        long mnum = Math.min(count, num);

        if (tlast >= mnum) {
            tlast = last - mnum;
        } else {
            tlast2 = capacity - tnum2;
            tnum2 = (int) (mnum - tlast);
        }
        try {
            raf.seek(HEADER_LEN + (tlast * recLen));
            bax = new byte[tnum * recLen];
            raf.read(bax);
            if (tlast2 > 0) {
                raf.seek(HEADER_LEN + (tlast2 * recLen));
                bax2 = new byte[tnum2 * recLen];
                raf.read(bax2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[][] ret = new byte[(bax.length + bax2.length) / recLen][recLen];
        for (int i = 0; i < tnum; i++) {
            ret[i] = copyOfRange(bax, i * recLen, (i + 1) * recLen);
        }
        for (int i = 0; i < tnum2; i++) {
            ret[i + tnum] = copyOfRange(bax2, i * recLen, (i + 1) * recLen);
        }
        // todo copy bax and bax2 to ret
        return ret;
    }


    public void delete() {
        if (count > 0) {
            count = count - 1;
            last = (last == 0 ? capacity : last) - 1;
            updateHeader();
        }
    }

    public void delete(int num) {
        if (count > 0) {
            long mnum = Math.min(count, num);
            count = count - mnum;
            last = last >= mnum ? last - mnum : capacity - (mnum - last);
            updateHeader();
        }
    }


    public long getCount() {
        return count;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getLast() {
        return last;
    }

    private void updateHeader() {
        try {
            raf.seek(4);
            raf.writeLong(count);
            raf.writeLong(last);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readHeader() throws IOException {
        raf.seek(4);
        count = raf.readLong();
        last = raf.readLong();

    }

    private void writeRecLen(int recLen) {
        try {
            raf.seek(0);
            raf.writeInt(recLen);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readRecLen() throws IOException {
        raf.seek(0);
        recLen = raf.readInt();
    }
}
