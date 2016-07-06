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

public class RingBuffer {

    public String dataFile;
    private long capacity;
    private long count;
    private long last;

    private byte recLen = 20;
    private static final byte headerLen = 16;
    private RandomAccessFile raf;


    public RingBuffer(String dataFile, long initCapacity) {
        this.dataFile = dataFile;
        capacity = initCapacity;
        try {
            raf = new RandomAccessFile(this.dataFile, "rw");
            setCapacity(capacity);
            count = 0;
            last = 0;
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
            capacity = (raf.length() - headerLen) / recLen;
            if (rafExits) {
                readHeader();
            } else {
                count = 0;
                last = 0;
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
            raf.setLength((capacity * recLen) + headerLen);
            updateHeader();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte getRecLen() {
        return recLen;
    }

    public void setRecLen(byte recLen) {
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
            raf.seek(headerLen + (last * recLen));
            raf.write(data);
            updateHeader();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] pop() {
        if (count > 0) {
            try {
                raf.seek(headerLen + (last * recLen));
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
                raf.seek(headerLen + (last * recLen));
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

        // public static byte[] copyOfRange(byte[] original, int from, int to)

        for (int i = 0; i < mnum; i++) {
            try {
                raf.seek(headerLen + (tlast * recLen));
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
            last = (last < mnum ? capacity : last) - mnum; // // TODO: -- wrong !
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
            raf.seek(0);
            raf.writeLong(count);
            raf.writeLong(last);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readHeader() throws IOException {
        raf.seek(0);
        count = raf.readLong();
        last = raf.readLong();

    }

}
