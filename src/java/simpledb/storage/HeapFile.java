package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	/** Unique ID. */
	private int id;
//	/** The number of the heap page in this heap file. */
//	private int numPage;
	/** The file that stores the on-disk backing store for this heap file. */
	private File file;
	/** The schema of the tuples stored in the HeapFile. */
	private TupleDesc tupleDesc;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here -Done
    	this.id = f.getAbsoluteFile().hashCode();
    	this.file = f;
    	this.tupleDesc = td;
    	// Question.
//    	this.numPage = (int) (f.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here -Done
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here -Done
    	return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here -Done
    	return this.tupleDesc;
    }

    // see DbFile.java for javadocs.
    public Page readPage(PageId pid) {
        // some code goes here -Done
    	if (!(pid instanceof HeapPageId)) {
    		return null;
    	}
    	
    	byte[] bytes = new byte[BufferPool.getPageSize()];
    	// Random access the file in the disk.
    	RandomAccessFile randomAccess = null;
		try {
			// Construct the stream.
			randomAccess = new RandomAccessFile(this.file, "r");
	    	// Find the start position of the pointer.
			randomAccess.seek(pid.getPageNumber() * BufferPool.getPageSize());
	    	randomAccess.read(bytes, 0, BufferPool.getPageSize());
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				randomAccess.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    	
    	HeapPageId pageId = new HeapPageId(this.id, pid.getPageNumber());
    	HeapPage page = null;
		try {
			page = new HeapPage(pageId, bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here  -Done
        // not necessary for lab1
    	byte[] data = page.getPageData();
    	RandomAccessFile randomAccess = new RandomAccessFile(this.file, "rw");
    	randomAccess.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
    	randomAccess.write(data);
    	randomAccess.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here -Done
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here -Done
    	// not necessary for lab1
    	
    	// Find a page with an empty slot.
    	BufferPool bufferPool = Database.getBufferPool();
    	ArrayList<Page> list = new ArrayList<>();
    	boolean noSuchPage = true;
    	for(int i = 0; i < this.numPages(); i++) {
    		PageId pageId = new HeapPageId(this.id, i);
    		HeapPage heapPage = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
    		if(heapPage.getNumEmptySlots() != 0) {
    			noSuchPage = false;
    			heapPage.insertTuple(t);
    			list.add(heapPage);
    		    break;
    		}
    	}
    	
    	// If no such pages exist in the heap file, create a new page and append it
    	// to the physical file on disk.
    	if (noSuchPage) {
    		HeapPage heapPage = new HeapPage(new HeapPageId(this.id, this.numPages()),
    				HeapPage.createEmptyPageData());
    		heapPage.insertTuple(t);
    		list.add(heapPage);
    		this.writePage(heapPage);
//    		this.numPage++;
    	}
        	
    	return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here -Done
    	// not necessary for lab1
    	ArrayList<Page> list = new ArrayList<>();
    	RecordId recordId = t.getRecordId();
    	PageId pageId = recordId.getPageId();
    	
    	if (this.id == pageId.getTableId()) {
    		HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
    		heapPage.deleteTuple(t);
    		list.add(heapPage);
    	} else {
    		throw new DbException("Tuple is not a member of the file");
    	}
    	return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here -Done
        return new HeapFileIterator(tid);
    }
    
    /**
     * The iterator of the heap file. This class encapsulation necessary variable of the
     * iterator.
     *
     */
    private class HeapFileIterator implements DbFileIterator {
    	
    	/** The page position of the iterator. */
    	private int iteratorPagePos;
    	/** Iterator the tuples in one page. */
    	private Iterator<Tuple> tupelIterator;
    	private TransactionId tid;
    	
    	/**
    	 * Constructor.
    	 * @param tid
    	 * 				do not know what it means in the lab1.
    	 */
    	public HeapFileIterator(TransactionId tid) {
    		this.tid = tid;
		}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.iteratorPagePos = 0;
			
			// Construct the first pageId, use this pageId to get the information
			// of the first page.
			HeapPageId pageId = new HeapPageId(getId(), this.iteratorPagePos);
			HeapPage page = (HeapPage) Database.getBufferPool()
					        .getPage(this.tid, pageId, Permissions.READ_ONLY);
			this.tupelIterator = page.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// Because we must use the 'tupelIterator', so we need to judge if it is null to
			// avoid the nullPointerException.
			if (this.tupelIterator == null) {
				return false;
			}
			
			// The there exists more pages.
			if (this.iteratorPagePos < numPages() - 1 && this.iteratorPagePos >= 0) {
				if(this.tupelIterator.hasNext()) {
					return true;
				} else {
					tupleIteratorMove();
//					return true;
					return this.tupelIterator.hasNext();
				}
			// The last page.
			} else if(this.iteratorPagePos == numPages() - 1) {
				return this.tupelIterator.hasNext();
			// Other situation.
			} else {
				return false;
			}
		}
		
		/**
		 * Move the tuple iterator from the end of a page to the next page. In fact, it is
		 * to get the next page tuple iterator.
		 */
		private void tupleIteratorMove() {
			/*
			 *  Construct the pid. we must get the heapPage by the pid.
			 */
			// let the page position of the iterator move to the next.
			this.iteratorPagePos++;
			// The heapFile id is same as the table id.
			HeapPageId pageId = new HeapPageId(getId(), this.iteratorPagePos);
			
			// Get the next page.
			HeapPage heapPage = null;
			try {
				heapPage = (HeapPage) Database.getBufferPool()
						   .getPage(this.tid, pageId, Permissions.READ_ONLY);
				this.tupelIterator = heapPage.iterator();
			} catch (TransactionAbortedException e) {
				e.printStackTrace();
			} catch (DbException e) {
				e.printStackTrace();
			}
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// It has closed.
			if(this.tupelIterator == null) {
				throw new NoSuchElementException();
			} else {
				// tupelIterator need to change from one page to another page.
				if (hasNext()) {
					return this.tupelIterator.next();
				}
				throw new NoSuchElementException();
			}
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// In fact, the main function of the 'open()' method is to set the 
			// variable(iteratorPagePos, tupelIterator) to the start position.
			open();
		}

		@Override
		public void close() {
			// Those two variable description the process which iterate the tuples 
			// among the pages in the heap file.
			this.iteratorPagePos = 0;
			this.tupelIterator = null;
		}
    	
    }
}

