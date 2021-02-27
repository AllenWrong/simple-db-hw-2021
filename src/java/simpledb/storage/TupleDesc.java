package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	private ArrayList<TDItem> tupleSchema;
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here -Done
        return this.tupleSchema.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here -Done
    	this.tupleSchema = new ArrayList<>();
    	
    	try {
    		// If the length of the type array is less than one, throw a exception
    		// and deal with it immediately.
			if (typeAr.length < 1) {
				throw new Exception("The numbe of the types of fields is less than"
						+ "one when construct tupleDesc");
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
    	
    	// Construct this tupleSchema
    	for(int i = 0; i < typeAr.length; i++) {
    		this.tupleSchema.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    	
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.tupleSchema = new ArrayList<>();
    	
    	try {
    		// If the length of the type array is less than one, throw a exception
    		// and deal with it immediately.
			if (typeAr.length < 1) {
				throw new Exception("The numbe of the types of fields is less than"
						+ "one when construct tupleDesc");
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
    	
    	// Construct this tupleSchema
    	for(int i = 0; i < typeAr.length; i++) {
    		this.tupleSchema.add(new TDItem(typeAr[i], null));
    	}
    	
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here -Done
        return this.tupleSchema.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here -Done
    	return this.tupleSchema.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here -Done
    	return this.tupleSchema.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here -Done
    	// Here, using the 'this.tupleSchema.size()' instead of the 'numFields()'
    	// can reduce the dependence and avoid the cascade mistakes.
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
    	
    	for(int i = 0; i < this.tupleSchema.size(); i++) {
    		if(name.equals(this.tupleSchema.get(i).fieldName)) {
    			return i;
    		}
    	}
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here -Done
    	int totalSize = 0;
    	// Here, using the 'this.tupleSchema.size()' instead of the 'numFields()'
    	// can reduce the dependence and avoid the cascade mistakes.
    	for(int i = 0; i < this.tupleSchema.size(); i++) {
    		totalSize += this.tupleSchema.get(i).fieldType.getLen();
    	}
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here -Done
    	ArrayList<Type> typesList = new ArrayList<>();
    	ArrayList<String> stringsList = new ArrayList<>();
    	
    	for(int i = 0; i < td1.numFields(); i++) {
    		typesList.add(td1.getFieldType(i));
    		stringsList.add(td1.getFieldName(i));
    	}
    	
    	for(int i = 0; i < td2.numFields(); i++) {
    		typesList.add(td2.getFieldType(i));
    		stringsList.add(td2.getFieldName(i));
    	}
    	
    	// Directly using the 'toArray', such as '(String[])stringsList.toArray()',
    	// will throw exception and i do not know the reason.
    	Type[] typeArr = new Type[typesList.size()];
    	typesList.toArray(typeArr);
    	
    	String[] fieldArr = new String[stringsList.size()]; 
    	fieldArr = stringsList.toArray(fieldArr);
    	
    	TupleDesc tupleDesc = new TupleDesc(typeArr, fieldArr);
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here -Done
    	if (o == null) {
    		return false;
    	}
    	
    	TupleDesc object = null;
    	
    	if(o instanceof TupleDesc) {
    		object = (TupleDesc)o;
    	} else {
    		return false;
    	}
    	
    	
    	if (this.tupleSchema.size() == object.numFields()) {
    		for (int i = 0; i <this.tupleSchema.size(); i++) {
    			if(!this.tupleSchema.get(i).fieldType.equals(object.getFieldType(i))) {
    				return false;
    			}
    		}
    		return true;
    	} else {
    		return false;
    	}
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here -Done
    	String tupleSchemaLine = "";
    	for (int i = 0; i < this.tupleSchema.size(); i++) {
    		String subline = "";
    		if (i == this.tupleSchema.size()-1) {
    			subline += this.tupleSchema.get(i).fieldType.toString()+"[" + i +
    					   "]("+ this.tupleSchema.get(i).fieldName +"[" + i + "])";
    		} else {
    			subline += this.tupleSchema.get(i).fieldType.toString()+"[" + i +
 					       "]("+ this.tupleSchema.get(i).fieldName +"[" + i + "]),";
    		}
    		tupleSchemaLine += subline;
    	}
        return tupleSchemaLine;
    }
}

