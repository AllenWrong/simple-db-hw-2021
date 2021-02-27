package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.RecordId;
import simpledb.storage.StringField;
import simpledb.storage.TupleDesc;
import simpledb.common.Type;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Tuple description of this tuple.*/
    private TupleDesc tupleDesc;
    
    /** The RecordId representing the location of this tuple on disk. May be null.*/
    private RecordId recordId;
    
    private ArrayList<Field> tuple;
    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here -Done
    	try {
			if(td.numFields() < 1) {
				throw new Exception("Less than one field when construct tuple.");
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
    	
    	this.tuple = new ArrayList<>();
    	
    	// Initialize the tuple with 0 or "".
    	for(int i = 0; i < td.numFields(); i++) {
    		if (td.getFieldType(i).equals(Type.INT_TYPE)) {
    			this.tuple.add(new IntField(0));
    		} else if(td.getFieldType(i).equals(Type.STRING_TYPE)) {
    			this.tuple.add(new StringField("", Integer.MAX_VALUE));
    		} else {
    			try {
					throw new Exception("Unkown Field Type when contruct tuple.");
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
    		}
    	}
    	this.tupleDesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here -Done
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here -Done
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here -Done
    	this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here -Done
    	if (i < 0 || i >= this.tuple.size()) {
    		System.err.println("The index in the setField is illegal.");
    	} else {
    		this.tuple.set(i, f);
    	}
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here -Done
    	if (i < 0 || i >= this.tuple.size()) {
    		System.err.println("The index in the setField is illegal.");
    	} else {
    		return this.tuple.get(i);
    	}
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here -Done
    	int tupleLength = this.tuple.size();
    	String tupleLine = "";
    	for(int i = 0; i < tupleLength; i++) {
    		// Format the output.
    		if(i == tupleLength - 1) {
    			// If here needs a '\n'?
    			tupleLine += (getField(i) + "\n");
    		} else {
    			tupleLine += (getField(i) + "\t");
    		}
    	}
    	return tupleLine;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here -Done
    	return this.tuple.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here -Done
    	this.tupleDesc = td;
    }
}
