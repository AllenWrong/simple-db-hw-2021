package simpledb.storage;

import simpledb.common.LockManager;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.storage.RecordId;
import simpledb.storage.Tuple;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    /** 
     * The bufferPool array is used to store the page. In fact, it represents
     * the whole buffer in the memory. It will be initialized by the numPages.
     * Because it's length is same as the number of the pages.
     */
    private Page[] bufferPool;
    /**
     * Store the map between the pageID and the index of the heapPage in the
     * buffer pool(the array above). 
     */
    private HashMap<PageId, Integer> pageIDToIndex;
    /** Used to label if the page is in the buffer pool. */
    private int[] exist;
    private int emptyPageLength;
    
    /** Used to evict page. */
    private LRU lru;
    
    private LockManager lock;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here -Done
    	this.bufferPool = new Page[numPages];
    	this.exist = new int[numPages];
    	this.emptyPageLength = numPages;
    	this.pageIDToIndex = new HashMap<>();
    	this.lru = new LRU(numPages);
    	this.lock = new LockManager(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here -Done
    	/* 
    	 * The other function of the pageIDToIndex is following.
    	 * Use it to judge if the page exists in the buffer.
    	 */
    	// Page exists in the buffer.
    	if (this.pageIDToIndex.containsKey(pid)) {
    		int index = this.pageIDToIndex.get(pid);
    		
    		// If pageIDToIndex contains the key, the index must in the
    		// LRURefer.
    		this.lru.leftMove(this.lru.findIndexOf(index) + 1);
    		this.lru.fill(index);
    		
    		try {
				lock.acquire(tid, index, perm);
			} catch (InterruptedException e) {
				throw new TransactionAbortedException();
			}
    		return this.bufferPool[index];
    		
    	// Page is not in the buffer. Add page, or evict page.
    	} else {
    		if(this.emptyPageLength != 0) {
    			
    			// The buffer is not full. Read the page from the disk by 
    			// the 'HeapFile' and find a physical frame to put the page.
    			for(int i = 0; i < this.bufferPool.length; i++) {
    				if(exist[i] == 0) {
    					DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//    					HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
    					Page page = dbFile.readPage(pid);
    					this.addPage(i, page);
    		    		try {
    						lock.acquire(tid, i, perm);
    					} catch (InterruptedException e) {
    						throw new TransactionAbortedException();
    					}
    					return page;
    				}
    			}
    		} else {
    			/*
    			 * In later lab implements this process.
    			 * Check if it is dirty.
    			 * Follow the evict algorithm to change a page.
    			 */
    			this.evictPage();
    			for (int i = 0; i < this.bufferPool.length; i++) {
					if(this.exist[i] == 0) {
    					DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//    					HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
    					Page page = dbFile.readPage(pid);
    					this.addPage(i, page);
    		    		try {
    						lock.acquire(tid, i, perm);
    					} catch (InterruptedException e) {
    						throw new TransactionAbortedException();
    					}
    					return page;
					}
				}
    		}
		}
    	throw new TransactionAbortedException();
    }
    
    /**
     * Add a page to the buffer pool. Because of the way of the implementation of
     * the buffer pool I encapsulate this method. I used three main data structure
     * and one variable(emptyPageLength) to implement the buffer pool. I need to
     * change four variable when i change a page from buffer pool. This is so
     * tedious that I will forget one or two steps sometimes. So I encapsulate 
     * this method. 
     * @param pos
     * @param heapPage
     */
    private void addPage(int index, Page page) {
	    /*************************************************
	     *  Note: When add one page, we need to set four variable.
	     *  1. bufferPool: means that put the page into the buffer.
	     *  2. pageIDToIndex: means that memory the map of the pageID and index.
	     *  3. exist: set '1', means that this page exists in the buffer.
	     *  4. emptyPageLength: minus one.
	     ******************************************************/
    	this.bufferPool[index] = page;
    	this.pageIDToIndex.put(page.getId(), index);
    	this.exist[index] = 1;
    	this.emptyPageLength--;
    	
    	// If we can add page, the LRURefer must have empty position.
    	this.lru.fill(index);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here -Done
        // not necessary for lab1|lab2
    	for (int i = 0; i < this.bufferPool.length; i++) {
    		if (null != this.bufferPool[i] && this.bufferPool[i].getId().equals(pid)) {
    			if (lock.isHolding(tid, i)) {
    				lock.release(tid, i);
    				return;
    			}
    		}
    	}
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here -Done
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here -Done
        // not necessary for lab1|lab2
        for (int i = 0; i < this.bufferPool.length; i++) {
        	if (null != this.bufferPool[i] && this.bufferPool[i].getId().equals(p)) {
        		return lock.isHolding(tid, i);
        	}
        }
        
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        {
        // some code goes here -Done
        // not necessary for lab1|lab2
    	
    	/*
    	 * When you commit, you should flush dirty pages associated to the
    	 * transaction to disk. Whether the transaction commits or aborts,
    	 * you should also release any state the BufferPool keeps regarding
    	 * the transaction, including releasing any locks that the
    	 * transaction held.
    	 * When you abort, you should revert any changes made by the transaction
    	 * by restoring the page to its on-disk state.
    	 */
    	if (commit) {
    		try {
				flushPages(tid);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	
		for (int i = 0; i < this.bufferPool.length; i++) {
			if (this.exist[i] == 1) {
				// The page is dirtied by this transaction.
				if (!commit && tid.equals(this.bufferPool[i].isDirty())) {
					for (Entry<PageId, Integer> entry : this.pageIDToIndex.entrySet()) {
						if (entry.getValue() == i) {
							// Change the this.lru to keep up with the state of
							// the current buffer pool.
							this.lru.leftMove(this.lru.findIndexOf(i) + 1);
							discardPage(entry.getKey());
							break;
						}
					}
				}
				
				if (lock.isHolding(tid, i)) {
					lock.release(tid, i);
				}
			}
		}
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here -Done
        // not necessary for lab1
    	
    	ArrayList<Page> list = (ArrayList<Page>) Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	for(Page page : list) {

    		// The page is in buffer.
    		if(this.pageIDToIndex.containsKey(page.getId())) {
    			int index = this.pageIDToIndex.get(page.getId());
    			this.bufferPool[index].markDirty(true, tid);
    		
    		// The page is not in buffer and the buffer pool has empty buffer.
    		} else if(this.emptyPageLength != 0) {
    			for(int i = 0; i < this.bufferPool.length; i++) {
    				if(this.exist[i] == 0) {
    					page.markDirty(true, tid);
    					this.addPage(i, (HeapPage)page);
    					break;
    				}
    			}
    		// Buffer Pool is full.
    		} else {
    			// evict page.
    			this.evictPage();
    			for (int i = 0; i < this.bufferPool.length; i++) {
					if(this.exist[i] == 0) {
//    					HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
//    					HeapPage heapPage = (HeapPage) heapFile.readPage(page.getId());
						DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
    					Page page2 = dbFile.readPage(page.getId());
    					page2.markDirty(true, tid);
    					this.addPage(i, page2);
					}
				}
    		}
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here -Done
        // not necessary for lab1
    	
    	RecordId recordId = t.getRecordId();
//    	HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
    	DbFile dbFile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
    	List<Page> list = dbFile.deleteTuple(tid, t);
    	for(Page page : list) {
    		page.markDirty(true, tid);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here -Done
        // not necessary for lab1
    	for(int i = 0; i < this.bufferPool.length; i++) {
    		if(this.exist[i] == 1 && this.bufferPool[i].isDirty() != null) {
        		this.flushPage(this.bufferPool[i].getId());
        	}
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here -Done
        // not necessary for lab1
    	if (this.pageIDToIndex.containsKey(pid)) {
        	int index = this.pageIDToIndex.get(pid);
        	this.bufferPool[index] = null;
        	this.pageIDToIndex.remove(pid);
        	this.exist[index] = 0;
        	this.emptyPageLength++;
    	}
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here -Done
        // not necessary for lab1
    	/*
    	 * flushPage should write any dirty page to disk and mark it as 
    	 * not dirty, while leaving it in the BufferPool.
    	 */
    	int tableId = pid.getTableId();
    	Page page = this.bufferPool[this.pageIDToIndex.get(pid)];
    	
    	if(page.isDirty() != null) {
    		Database.getCatalog().getDatabaseFile(tableId).writePage(page);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here -Done
        // not necessary for lab1|lab2
    	for (int i = 0; i < this.bufferPool.length; i++) {
    		if (this.exist[i] == 1 && lock.isHolding(tid, i)) {
    		    // append an update record to the log, with 
    		    // a before-image and after-image.
    		    TransactionId dirtier = this.bufferPool[i].isDirty();
    		    if (dirtier != null){
    		      Database.getLogFile().logWrite(dirtier, this.bufferPool[i].getBeforeImage(), this.bufferPool[i]);
    		      Database.getLogFile().force();
    		    }
    		    
    			flushPage(this.bufferPool[i].getId());
    			
    			// use current page contents as the before-image
    			// for the next transaction that modifies this page.
    			this.bufferPool[i].setBeforeImage();
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here -Done
        // not necessary for lab1

    	// Using steal policy need to comment out the following four
    	// line code. And change the "index + 1" to "1".
//    	int index = this.lru.findIdxOfclrPage();
//    	if (index == -1) {
//    		throw new DbException("All pages in the buffer pool are dirty");
//    	}
    	
//    	int removedPageIndex = this.lru.leftMove(index + 1);
    	int removedPageIndex = this.lru.leftMove(1);
    	PageId heapPageId = null;
    	Set<Entry<PageId, Integer>> set = this.pageIDToIndex.entrySet();
    	for(Entry<PageId, Integer> entry : set) {
    		if(entry.getValue() == removedPageIndex) {
    			heapPageId = entry.getKey();
    			break;
    		}
    	}
    	
//    	try {
//			this.flushPage(heapPageId);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
    	
    	this.discardPage(heapPageId);
    }
    
	private class LRU {
		/** The element is the index of page in the buffer pool. */
		private int[] LRURefer;
		/** The next element after the last element. */
		private int endPos;
		
		public LRU(int size) {
			this.LRURefer = new int[size];
			for(int i = 0; i < size; i++) {
				this.LRURefer[i] = -1;
			}
			// There is no element at start.
			this.endPos = 0;
		}
		
		/**
		 * Left move the element in the LRURefer. This method must be synchronized
		 * or it will lead to the error endPos.
		 * @param start the index of the first element needed to move. If it is
		 *              -1, it will move the whole array. 
		 */
		public synchronized int leftMove(int start) {
			int removedPageIndex = this.LRURefer[start - 1];
			for(int i = start; i < this.endPos; i++) {
				this.LRURefer[i - 1] = this.LRURefer[i];
			}
			this.endPos--;
			return removedPageIndex;
		}
		
		/**
		 * Find the index of the first element which represent the page
		 * that is not dirty. We evict the non-dirty page.
		 * 
		 * @return This will return a index. 
         *
		 */
		public int findIdxOfclrPage() {
			for (int i = 0; i < this.endPos; i++) {
				if (bufferPool[this.LRURefer[i]].isDirty() == null) {
					return i;
				}
			}
			return -1;
		}
		
		/**
		 * Find the index of the specific element.
		 * @param element the specific element.
		 * @return If this element in this array, return the index of it or return -1;
		 */
		public int findIndexOf(int element) {
			for(int i = 0; i < this.endPos; i++) {
				if(this.LRURefer[i] == element) {
					return i;
				}
			}
			return -1;
		}
		
		/**
		 * Find the first empty position and return it's index.
		 * @return index of the first empty position. If the LRURefer is full return -1.
		 */
		@SuppressWarnings("unused")
		public int findEmptyPos() {
			if(this.endPos == this.LRURefer.length) {
				return -1;
			} else {
				return this.endPos;
			}
		}
		
		/**
		 * Fill the index in the specific position of the LRURefer.
		 * @param index The index of the specific page in the buffer pool.
		 */
		public void fill(int index) {
			this.LRURefer[this.endPos] = index;
			this.endPos++;
		}
		
		@SuppressWarnings("unused")
		@Deprecated
		public void see() {
			for(int index : this.LRURefer) {
				System.out.print(index+"\t");
			}
			System.out.println();
		}
	}

}