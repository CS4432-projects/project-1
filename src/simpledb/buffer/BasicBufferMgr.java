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

   /**
    * CS4432-Project1:
    * A LinkedList of the currently unpinned buffers.
    */
   private LinkedList<Buffer> availableBuffers;

   /**
    * CS4432-Project1:
    * A LinkedList of the currently empty buffers.
    */
   private LinkedList<Buffer> emptyBuffers;

   /**
    * CS4432-Project1:
    * A HashMap of blocks and their associated buffers.
    */
   private HashMap<Block, Buffer> mapBlockToBuffer;

   /**
    * CS4432-Project1:
    * The current index of the buffer pool used with the Clock replacement policy.
    */
   private int clockPointer = 0;
   
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
      emptyBuffers = new LinkedList<>();
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
         /**
          * CS4432-Project1:
          * Initially add all frames to available buffers.
          */
         availableBuffers.add(bufferpool[i]);
         emptyBuffers.add(bufferpool[i]);
         bufferpool[i].setBufferID(i);
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
    *
    * CS4432-Project1:
    * Added logic for removing the buffer from the list of available buffers and mapping the block to the associated buffer.
    * Added logic to set state based on the specified replacement policy.
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
      if (Startup.REPLACEMENT_POLICY.equals("LRU")) {
         buff.setLastUsed();
      } else { // CLOCK
         buff.refOn();
      }
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
    *
    * CS4432-Project1:
    * Added logic for removing the buffer from the list of available buffers and mapping the block to the associated buffer.
    * Added logic to set state based on the specified replacement policy.
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
      if (Startup.REPLACEMENT_POLICY.equals("LRU")) {
         buff.setLastUsed();
      } else { // CLOCK
         buff.refOn();
      }
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    *
    * CS4432-Project1:
    * Added logic for different replacement policies.
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (Startup.REPLACEMENT_POLICY.equals("LRU")) {
         buff.setLastUsed();
      } else { // CLOCK
         buff.refOn();
      }
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

   /**
    * CS4432-Project1:
    * Use HashMap to find existing blocks in the buffer.
    */
   private Buffer findExistingBuffer(Block blk) {
      Buffer buff = mapBlockToBuffer.get(blk);
      if (buff != null) {
         buff.setLastUsed();
      }
      return buff;
   }

   /**
    * CS4432-Project1:
    * Checks for empty frames first.
    * If no empty frames are available, chooses buffer based on specified replacement policy (either LRU or Clock.)
    */
   private Buffer chooseUnpinnedBuffer() {
      // check for empty frames
      if (emptyBuffers.size() > 0) {
         return emptyBuffers.removeFirst();
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
          boolean foundFrame = false;
          while (!foundFrame) {
             for (; clockPointer < bufferpool.length; clockPointer++) {
                Buffer buff = bufferpool[clockPointer];
                if (!buff.isPinned() && buff.getRefBit() == 1) {
                   buff.refOff();
                } else if (!buff.isPinned() && buff.getRefBit() == 0) {
                   return buff;
                }
             }
             if (clockPointer >= bufferpool.length) {
                clockPointer = 0;
             }
          }
          return availableBuffers.getFirst();
      }
   }

   /**
    * CS4432-Project1:
    * Prints out a string representation of the buffer pool.
    */
   public String toString() {
      String result = "";
      for (int i = 0; i < bufferpool.length; i++) {
         result += bufferpool[i].toString();
      }
      return result;
   }
}
