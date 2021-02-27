package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	/** The table id that is being referenced. */
	private int tableId;
	/** The page serial number in that table. */
	private int pageSerialNum;
	
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here -Done
    	this.tableId = tableId;
    	this.pageSerialNum = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here -Done
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here -Done
        return this.pageSerialNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here -Done
//    	String tableIDStr = this.tableId + "";
//    	String pageSerialNumStr = this.pageSerialNum + "";
//    	String concatenation = tableIDStr + pageSerialNumStr;
//    	return Integer.valueOf(concatenation);
    	// tableId left move 6 bit.
    	return this.tableId * 64 + this.pageSerialNum;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here -Done
    	if(o instanceof HeapPageId) {
    		HeapPageId heapPageId = (HeapPageId)o;
    		// Same tableId and same page serial number.
    		if(this.tableId == heapPageId.getTableId() &&
    		   this.pageSerialNum == heapPageId.getPageNumber()) {
    			return true;
    		} else {
    			return false;
    		}
    		
    	} else {
    		return false;
		}
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
