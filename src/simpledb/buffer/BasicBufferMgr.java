package simpledb.buffer;

import simpledb.file.*;
import simpledb.server.Startup;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;

   private LinkedList<Buffer> availableBuffers;

   private HashMap<Block, Buffer> mapBlockToBuffer;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      availableBuffers = new LinkedList<>();
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         availableBuffers.add(bufferpool[i]);
      }
      mapBlockToBuffer = new HashMap<>();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         mapBlockToBuffer.remove(buff.block());
         buff.assignToBlock(blk);
         mapBlockToBuffer.put(buff.block(), buff);
      }
      if (!buff.isPinned()) {
         availableBuffers.remove(buff);
      }
      buff.pin();
      buff.setLastUsed();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      mapBlockToBuffer.remove(buff.block());
      buff.assignToNew(filename, fmtr);
      mapBlockToBuffer.put(buff.block(), buff);
      availableBuffers.remove(buff);
      buff.pin();
      buff.setLastUsed();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      buff.setLastUsed();
      if (!buff.isPinned()) {
         availableBuffers.add(buff);
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return availableBuffers.size();
   }
   
   private Buffer findExistingBuffer(Block blk) {
      Buffer buff = mapBlockToBuffer.get(blk);
      if (buff != null) {
         buff.setLastUsed();
      }
      return buff;
   }
   
   private Buffer chooseUnpinnedBuffer() {
      // check for empty frames
      for (Buffer buff : availableBuffers) {
         if (buff.block() == null) {
            return buff;
         }
      }
      if (Startup.REPLACEMENT_POLICY.equals("LRU")) {
          // find least recently used
          Buffer min = availableBuffers.getFirst();
          for (Buffer buff : availableBuffers) {
              if (buff.getLastUsed() < min.getLastUsed()) {
                  min = buff;
              }
          }
          return min;
      } else {//CLOCK
          return availableBuffers.getFirst();
      }
   }
}
