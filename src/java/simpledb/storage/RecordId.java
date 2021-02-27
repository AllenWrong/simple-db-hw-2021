package simpledb.storage;

import java.io.Serializable;

import simpledb.storage.PageId;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /** The page id of the page on which the tuple resides. */
    private PageId pageId;
    /** the tuple id within the page. */
    private int tupleid;
    
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here -Done
    	this.pageId = pid;
    	this.tupleid = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here -Done
        return this.tupleid;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here -Done
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here -Done
    	if(o instanceof RecordId) {
    		RecordId recordId = (RecordId)o;
    		// Same pageId and same TupleID.
    		if(this.pageId.equals(recordId.getPageId()) &&
    		   this.tupleid == recordId.getTupleNumber()) {
    			return true;
    		} else {
    			return false;
    		}
    		
    	} else {
    		return false;
		}
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here -Done
    	return Integer.valueOf(this.pageId.hashCode() + "" + this.tupleid);
//      throw new UnsupportedOperationException("implement this");
    }
}
