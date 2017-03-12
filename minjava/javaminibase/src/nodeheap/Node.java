/* File Tuple.java */

package nodeheap;

import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.*;
import java.lang.*;
import global.*;

public class Node extends Tuple {

    /**
     * Maximum size of any tuple
     */
    public static final int max_size = 74;

    /**
     * length of this tuple
     */
    private int node_length;

    /**
     * Class constructor Creat a new tuple with length = max_size,tuple offset =
     * 0.
     * @throws IOException 
     * @throws InvalidTupleSizeException 
     * @throws InvalidTypeException 
     */

    public Node() throws InvalidTypeException, InvalidTupleSizeException, IOException {
        // Creat a new tuple
    	super(max_size);
        node_length = max_size;
        AttrType[] attrs = new AttrType[2];
        short[] str_sizes = new short[1];
        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrDesc);
        str_sizes[0] = (short) 44;
        this.setHdr((short)2, attrs, str_sizes);
    }

    /**
     * Constructor
     *
     * @param anode
     *            a byte array which contains the tuple
     * @param offset
     *            the offset of the tuple in the byte array
     *
     *            the length of the tuple
     * @throws IOException 
     * @throws InvalidTupleSizeException 
     * @throws InvalidTypeException 
     */

    public Node(byte[] anode, int offset) {
        super(anode,offset,max_size);
        node_length = max_size;
        // fldCnt = getShortValue(offset, data);
        AttrType[] attrs = new AttrType[2];
        short[] str_sizes = new short[1];
        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrDesc);
        str_sizes[0] = (short) 44;
        try {
			this.setHdr((short)2, attrs, str_sizes);
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Constructor(used as tuple copy)
     *
     * @param fromNode
     *            a byte array which contains the tuple
     *
     */
    public Node(Node fromNode) {
    	super(fromNode);
        node_length = fromNode.getLength();
    }

    /**
     * Class constructor Creat a new tuple with length = size,tuple offset = 0.
     */
//
//    public Node(int size) {
//        // Creat a new tuple
//        data = new byte[size];
//        node_offset = 0;
//        node_length = size;
//    }

    /**
     * Copy a tuple to the current tuple position you must make sure the tuple
     * lengths must be equal
     *
     * @param fromNode
     *            the tuple being copied
     */
    public void nodeCopy(Node fromNode) {
    	tupleCopy(fromNode);
    }

    /**
     * This is used when you don't want to use the constructor
     *
     * @param anode
     *            a byte array which contains the tuple
     * @param offset
     *            the offset of the tuple in the byte array
     *
     *            the length of the tuple
     */

    public void nodeInit(byte[] anode, int offset) {
        node_length = max_size;
    }
    
    
	/**
	 * return the data byte array
	 * 
	 * @return data byte array
	 */

	public byte[] returnNodeByteArray() {
		return returnTupleByteArray();
	}


    /**
     * Set a tuple with the given tuple length and offset
     *
     * @param record
     *            a byte array contains the tuple
     * @param offset
     *            the offset of the tuple ( =0 by default)
     *
     *            the length of the tuple
     */
    public void nodeSet(byte[] record, int offset) {
        int length = max_size;
        tupleSet(record, offset, max_size);
        node_length = length;
    }

    /**
     * get the length of a tuple, call this method if you did not call setHdr ()
     * before
     *
     * @return length of this tuple in bytes
     */
    public int getLength() {
        return node_length;
    }



    /**
     * Copy the tuple byte array out
     *
     * @return byte[], a byte array contains the tuple the length of byte[] =
     *         length of the tuple
     */

    public byte[] getNodeByteArray() {

        return getTupleByteArray();
    }

    /**
     * return the data byte array
     *
     * @return data byte array
     */
//
//    public byte[] returnTupleByteArray() {
//        return data;
//    }



    public String getLabel() throws IOException, heap.FieldNumberOutOfBoundException {
            return getStrFld(1);
    }

    public Descriptor getDesc() throws IOException, heap.FieldNumberOutOfBoundException {
        return getDescFld(2);
    }





    public Node setLabel(String val) throws IOException, heap.FieldNumberOutOfBoundException {
        return (Node)setStrFld(1, val);
    }

    public Node setDesc(Descriptor desc) throws IOException, heap.FieldNumberOutOfBoundException {
        return (Node)setDescFld(2, desc);
    }


    public void print() throws IOException, heap.FieldNumberOutOfBoundException {
        System.out.println("Node label"+ getLabel());
        System.out.println("Node descriptor values are");
        Descriptor desc = getDesc();
        for(int i= 0; i<5;i++){
            System.out.print(desc.get(i)+"  ");
        }
    }





}
