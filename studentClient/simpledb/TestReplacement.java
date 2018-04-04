import simpledb.file.Block;
import simpledb.server.Startup;

/**
 * Created by Kevin O'Brien on 4/4/2018.
 */
public class TestReplacement {

    public static void main(String[] args) {
        System.out.println("Buffer Replacement from HW2");
        System.out.println("LRU Replacement Policy:");
        Startup.REPLACEMENT_POLICY = "LRU";
        runBufferTest();
        System.out.println("Clock Replacement Policy:");
        Startup.REPLACEMENT_POLICY = "CLOCK";
        runBufferTest();
    }

    public static void runBufferTest() {
        TestBufferMgr testmgr = new TestBufferMgr(7);

        Block blk1 = new Block("", 1);
        Block blk2 = new Block("", 2);
        Block blk3 = new Block("", 3);
        Block blk4 = new Block("", 4);
        Block blk5 = new Block("", 5);
        Block blk6 = new Block("", 6);
        Block blk7 = new Block("", 7);
        Block blk8 = new Block("", 8);
        Block blk9 = new Block("", 9);
        Block blk10 = new Block("", 10);

        testmgr.pin(blk1);
        System.out.println(testmgr);
        testmgr.pin(blk2);
        System.out.println(testmgr);
        testmgr.pin(blk3);
        System.out.println(testmgr);
        testmgr.unpin(testmgr.findExistingBuffer(blk1));
        System.out.println(testmgr);
        testmgr.pin(blk4);
        System.out.println(testmgr);
        testmgr.pin(blk5);
        System.out.println(testmgr);
        testmgr.unpin(testmgr.findExistingBuffer(blk3));
        System.out.println(testmgr);
        testmgr.unpin(testmgr.findExistingBuffer(blk4));
        System.out.println(testmgr);
        testmgr.findExistingBuffer(blk1);
        System.out.println(testmgr);
        testmgr.pin(blk6);
        testmgr.unpin(testmgr.findExistingBuffer(blk6));
        System.out.println(testmgr);
        testmgr.pin(blk7);
        testmgr.unpin(testmgr.findExistingBuffer(blk7));
        System.out.println(testmgr);
        testmgr.pin(blk8);
        System.out.println(testmgr);
        testmgr.pin(blk9);
        System.out.println(testmgr);
        testmgr.findExistingBuffer(blk5);
        System.out.println(testmgr);
        testmgr.pin(blk10);
        testmgr.unpin(testmgr.findExistingBuffer(blk10));
        System.out.println(testmgr);
    }
}
