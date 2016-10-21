package ch.luethi.ringbuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by luethiand on 21.06.2016.
 */
public class RingBufferTest {

    private RingBuffer rb;
    private static final String TEST_DATA_FILE = "testdata.dat";
    private static final int DEF_REC_LEN = 20;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {
        deleteDtataFile();
    }

    private void deleteDtataFile() {
        if (rb != null) {
            rb.close();
        }
        File file = new File(TEST_DATA_FILE);
        if (file.delete()) {
            System.out.println(testName.getMethodName() + ", file '" + file.getName() + "' is deleted!");
        }
    }

    @After
    public void tearDown() throws Exception {
        rb.close();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void openExistingStorageTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 234, DEF_REC_LEN);
        assertEquals("Capacity not correct", 234, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        rb.close();

        rb = new RingBuffer(TEST_DATA_FILE, 234, DEF_REC_LEN, false);
        assertEquals("Capacity not correct", 234, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
    }

    @Test
    public void reopenStorageTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 1234, DEF_REC_LEN);
        assertEquals("Capacity not correct", 1234, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        for (int i = 0; i < ba.length; i++) {
            ba[i] = (byte) (i);
        }
        rb.push(ba);
        rb.close();

        rb = new RingBuffer(TEST_DATA_FILE, 1234, DEF_REC_LEN, false);
        assertEquals("Capacity not correct", 1234, rb.getCapacity());
        assertEquals("Count not correct", 1, rb.getCount());
        assertEquals("Last not correct", 1, rb.getLast());

        byte[]ba2 = rb.pop();
        assertEquals("Capacity not correct", 1234, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        assertEquals("Record size not correct", ba.length, ba2.length);
        assertArrayEquals("Record content not correct", ba, ba2);
    }

    @Test
    public void initialOpenStorageTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 200, DEF_REC_LEN);
        rb.close();
        rb = new RingBuffer(TEST_DATA_FILE, 200, DEF_REC_LEN, false);
        assertEquals("Capacity not correct", 200, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
    }

    @Test
    public void setCapacityTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 70, DEF_REC_LEN);
        rb.setCapacity(4711);
        assertEquals("Capacity not correct", 4711, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
    }

    @Test
    public void recLenTest() {
        byte[] ba = new byte[123];
        rb = new RingBuffer(TEST_DATA_FILE, 20, ba.length);
        assertEquals("Capacity not correct", 20, rb.getCapacity());
        assertEquals("Reclen not correct", ba.length, rb.getRecLen());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        for (byte i = 0; i < ba.length; i++) {
            ba[i] = (byte) (i % 50);
        }
        rb.push(ba);
        byte[] ba2 = rb.pop();
        assertEquals("Record size not correct", ba.length, ba2.length);
        assertArrayEquals("Record content not correct", ba, ba2);
    }

    @Test
    public void setRecLenTest() {
        byte[] ba = new byte[50];
        rb = new RingBuffer(TEST_DATA_FILE, 70, DEF_REC_LEN);
        rb.setCapacity(20);
        rb.setRecLen(ba.length);
        assertEquals("Capacity not correct", 20, rb.getCapacity());
        assertEquals("Reclen not correct", ba.length, rb.getRecLen());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        for (byte i = 0; i < ba.length; i++) {
            ba[i] = (byte) (i % 50);
        }
        rb.push(ba);
        byte[] ba2 = rb.pop();
        assertEquals("Record size not correct", ba.length, ba2.length);
        assertArrayEquals("Record content not correct", ba, ba2);
    }


    @Test
    public void pushWith_0_CapacityTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 0, DEF_REC_LEN);
        rb.setRecLen(ba.length);
        assertEquals("Capacity not correct", 0, rb.getCapacity());
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Storage capacity is 0");
        rb.push(ba);
    }


    @Test
    public void pushPopTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 100, ba.length);
        long last = rb.getLast();
        rb.push(ba);
        assertEquals("Count not correct", 1, rb.getCount());
        assertEquals("Last not correct", last + 1, rb.getLast());

        rb = new RingBuffer(TEST_DATA_FILE, 100, ba.length, false);
        byte[] ba2 = rb.pop();
        assertEquals("Record size not correct", ba.length, ba2.length);
        assertArrayEquals("Record content not correct", ba, ba2);

        byte[] ba3 = {11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120};
        rb.push(ba3);
        assertEquals("Count not correct", 1, rb.getCount());
        assertEquals("Last not correct", last + 1, rb.getLast());

        rb.close();
        rb = new RingBuffer(TEST_DATA_FILE, 100, ba.length, false);

        ba2 = rb.pop();
        assertEquals("Record size not correct", ba2.length, ba3.length);
        assertArrayEquals("Record content not correct", ba2, ba3);
    }


    @Test
    public void pushPeekDeleteTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 200, ba.length);
        rb.push(ba);
        long lastcnt = rb.getCount();
        long last = rb.getCount();
        rb.close();

        rb = new RingBuffer(TEST_DATA_FILE, 200, ba.length, false);
        byte[] ba2 = rb.peek();
        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        assertEquals("Record size not correct", ba.length, ba2.length);
        assertArrayEquals("Record content not correct", ba, ba2);

        rb.delete();
        assertEquals("Count not correct", lastcnt - 1, rb.getCount());
        assertEquals("Last not correct", last - 1, rb.getLast());
    }


    @Test
    public void pushPeekDeleteMultipleTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 50, ba.length );
        for (byte i = 0; i < 10; i++) {
            ba[0] = i;
            rb.push(ba);
        }
        long lastcnt = rb.getCount();
        long last = rb.getCount();
        rb.close();

        rb = new RingBuffer(TEST_DATA_FILE, 50, ba.length,false );
        byte[][] baa2 = rb.peek((byte) 5);
        assertEquals("Wrong amount peeked", 5, baa2.length);

        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        for (byte i = 0; i < 5; i++) {
            assertEquals("Record size not correct", ba.length, baa2[i].length);
            assertEquals("Record content not correct", 9 - i, baa2[i][0]);
        }

        rb.close();
        rb = new RingBuffer(TEST_DATA_FILE, 50, ba.length,false);
        baa2 = rb.peek(4);
        assertEquals("Wrong amount peeked", 4, baa2.length);

        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        for (byte i = 0; i < 4; i++) {
            assertEquals("Record size not correct", ba.length, baa2[i].length);
            assertEquals("Record content not correct", 9 - i, baa2[i][0]);
        }

        baa2 = rb.peek(20);
        assertEquals("Wrong amount peeked", 10, baa2.length);

        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        for (byte i = 0; i < 10; i++) {
            assertEquals("Record size not correct", ba.length, baa2[i].length);
            assertEquals("Record content not correct", 9 - i, baa2[i][0]);
        }
    }

    @Test
    public void pushPeek2DeleteMultipleTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 50, ba.length);
        for (byte i = 0; i < 10; i++) {
            ba[0] = i;
            rb.push(ba);
        }
        long lastcnt = rb.getCount();
        long last = rb.getCount();
        rb.close();

        rb = new RingBuffer(TEST_DATA_FILE, 50, ba.length, false);
        byte[][] baa2 = rb.peek2((byte) 5);
        assertEquals("Wrong amount peeked", 5, baa2.length);

        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        for (byte i = 0; i < 5; i++) {
            assertEquals("Record size not correct", ba.length, baa2[i].length);
            //todo wip: assertEquals("Record content not correct", 9 - i, baa2[i][0]);
        }

        rb.close();
        rb = new RingBuffer(TEST_DATA_FILE, 50, ba.length, false);
        baa2 = rb.peek2(4);
        assertEquals("Wrong amount peeked", 4, baa2.length);

        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        for (byte i = 0; i < 4; i++) {
            assertEquals("Record size not correct", ba.length, baa2[i].length);
            //todo wip: assertEquals("Record content not correct", 9 - i, baa2[i][0]);
        }

        baa2 = rb.peek2(20);
        assertEquals("Wrong amount peeked", 10, baa2.length);

        assertEquals("Count not correct", lastcnt, rb.getCount());
        assertEquals("Last not correct", last, rb.getLast());

        for (byte i = 0; i < 10; i++) {
            assertEquals("Record size not correct", ba.length, baa2[i].length);
            assertEquals("Record content not correct", 9 - i, baa2[i][0]);
        }
    }

    @Test
    public void wrongRecordSizeTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 100, 20);
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Date length error, length must be 20");
        assertEquals("Count not correct", 0, rb.getCount());
        rb.push(ba);
    }

    @Test
    public void wrapTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 100, ba.length);
        for (byte i = 0; i < 124; i++) {
            ba[0] = i;
            rb.push(ba);
        }
        assertEquals("Count not correct", 100, rb.getCount());
        assertEquals("Last not correct", 24, rb.getLast());

        byte c = 123;
        for (byte i = 0; i < 58; i++) {
            ba = rb.pop();
            assertEquals("Last not correct", c, ba[0]);
            c--;
        }
        assertEquals("Count not correct", 42, rb.getCount());
        assertEquals("Last not correct", 66, rb.getLast());
    }

    @Test
    public void underflowTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 10, ba.length);
        for (byte i = 0; i < rb.getCapacity(); i++) {
            ba[0] = i;
            rb.push(ba);
        }
        assertEquals("Count not correct", 10, rb.getCount());

        byte c = 9;
        for (byte i = 0; i < rb.getCapacity(); i++) {
            ba = rb.pop();
            assertEquals("Last not correct", c, ba[0]);
            c--;
        }
        assertEquals("Count not correct", 0, rb.getCount());
        ba = rb.pop();
        assertNull("Null reference expected", ba);
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        byte[] ba2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 88, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb.push(ba2);
        assertEquals("Count not correct", 1, rb.getCount());
        assertEquals("Last not correct", 1, rb.getLast());
        assertEquals("Last not correct", 88, ba2[9]);
    }

    @Test
    public void push_1_000_000_Test() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 1000000, ba.length);
        for (long i = 0; i < rb.getCapacity(); i++) {
            ba[0] = (byte) (i);
            rb.push(ba);
        }
        assertEquals("Count not correct", rb.getCapacity(), rb.getCount());
    }

    @Test
    public void deleteMultipleTest() {
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        rb = new RingBuffer(TEST_DATA_FILE, 30, ba.length);
        for (long i = 0; i < 20; i++) {
            ba[0] = (byte) (i);
            rb.push(ba);
        }
        assertEquals("Last not correct", 20, rb.getLast());
        rb.delete(8);
        assertEquals("Last not correct", 20-8, rb.getLast());
        rb.delete(16);
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());

        for (long i = 0; i < 34; i++) {
            ba[0] = (byte) (i);
            rb.push(ba);
        }
        assertEquals("Count not correct", 30, rb.getCount());
        assertEquals("Last not correct", 4, rb.getLast());
        rb.delete(16);
        assertEquals("Count not correct", 30-16, rb.getCount());
        assertEquals("Last not correct", 18, rb.getLast());

        rb.close();
        rb = new RingBuffer(TEST_DATA_FILE, 30, ba.length, false);
        assertEquals("Count not correct", 30-16, rb.getCount());
        assertEquals("Last not correct", 18, rb.getLast());
    }

    @Test
    public void deletStorageTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 234, DEF_REC_LEN);
        assertEquals("Capacity not correct", 234, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        for (int i = 0; i < ba.length; i++) {
            ba[i] = (byte) (i);
        }
        rb.push(ba);
        rb.close();

        deleteDtataFile();

        rb = new RingBuffer(TEST_DATA_FILE, 567, DEF_REC_LEN, false);
        assertEquals("Capacity not correct", 567, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
    }

    @Test
    public void initNewStorageTest() {
        rb = new RingBuffer(TEST_DATA_FILE, 432, DEF_REC_LEN);
        assertEquals("Capacity not correct", 432, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
        byte[] ba = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        for (int i = 0; i < ba.length; i++) {
            ba[i] = (byte) (i);
        }
        rb.push(ba);
        rb.close();

        rb = new RingBuffer(TEST_DATA_FILE, 987, DEF_REC_LEN, true);
        assertEquals("Capacity not correct", 987, rb.getCapacity());
        assertEquals("Count not correct", 0, rb.getCount());
        assertEquals("Last not correct", 0, rb.getLast());
    }




}