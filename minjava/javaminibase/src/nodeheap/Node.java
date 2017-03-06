/* File Tuple.java */

package nodeheap;

import heap.Tuple;

import java.io.*;
import java.lang.*;
import global.*;

public class Node extends Tuple {

    /**
     * Maximum size of any tuple
     */
    public static final int max_size = 64;

    /**
     * a byte array to hold data
     */
    private byte[] data;

    /**
     * start position of this tuple in data[]
     */
    private int node_offset;

    /**
     * length of this tuple
     */
    private int node_length;

    /**
     * private field Number of fields in this tuple
     */
    private short fldCnt;

    /**
     * private field Array of offsets of the fields
     */

    private short[] fldOffset;

    /**
     * Class constructor Creat a new tuple with length = max_size,tuple offset =
     * 0.
     */

    public Node() {
        // Creat a new tuple
        data = new byte[max_size];
        node_offset = 0;
        node_length = max_size;
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
     */

    public Node(byte[] anode, int offset) {
        data = anode;
        node_offset = offset;
        node_length = max_size;
        // fldCnt = getShortValue(offset, data);
    }

    /**
     * Constructor(used as tuple copy)
     *
     * @param fromNode
     *            a byte array which contains the tuple
     *
     */
    public Node(Node fromNode) {
        data = fromNode.getNodeByteArray();
        node_length = fromNode.getLength();
        //node_length = max_size;
        node_offset = 0;
        fldCnt = fromNode.noOfFlds();
        fldOffset = fromNode.copyFldOffset();
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
        byte[] temparray = fromNode.getNodeByteArray();
        System.arraycopy(temparray, 0, data, node_offset, node_length);
        // fldCnt = fromTuple.noOfFlds();
        // fldOffset = fromTuple.copyFldOffset();
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
        data = anode;
        node_offset = offset;
        node_length = max_size;
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
        System.arraycopy(record, offset, data, 0, length);
        node_offset = 0;
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
     * get the length of a tuple, call this method if you did call setHdr ()
     * before
     *
     * @return size of this tuple in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - node_offset));
    }

    /**
     * get the offset of a tuple
     *
     * @return offset of the tuple in byte array
     */
    public int getOffset() {
        return node_offset;
    }

    /**
     * Copy the tuple byte array out
     *
     * @return byte[], a byte array contains the tuple the length of byte[] =
     *         length of the tuple
     */

    public byte[] getNodeByteArray() {
        byte[] nodecopy = new byte[node_length];
        System.arraycopy(data, node_offset, nodecopy, 0, node_length);
        return nodecopy;
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
