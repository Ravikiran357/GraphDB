/* File Edge.java */

package edgeheap;

import java.io.*;
import java.lang.*;
import global.*;

public class Edge extends Tuple {

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
    private int edge_offset;

    /**
     * length of this tuple
     */
    private int edge_length;

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

    public Edge() {
        // Creat a new tuple
        data = new byte[max_size];
        edge_offset = 0;
        edge_length = max_size;
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

    public Edge(byte[] aedge, int offset) {
        data = aedge;
        edge_offset = offset;
        edge_length = max_size;
        // fldCnt = getShortValue(offset, data);
    }

    /**
     * Constructor(used as tuple copy)
     *
     * @param fromNode
     *            a byte array which contains the tuple
     *
     */
    public Edge(Edge fromEdge) {
        data = fromEdge.getEdgeByteArray();
        edge_length = fromEdge.getLength();
        //edge_length = max_size;
        edge_offset = 0;
        fldCnt = fromEdge.noOfFlds();
        fldOffset = fromEdge.copyFldOffset();
    }

    /**
     * Class constructor Creat a new tuple with length = size,tuple offset = 0.
     */
//
//    public Node(int size) {
//        // Creat a new tuple
//        data = new byte[size];
//        edge_offset = 0;
//        edge_length = size;
//    }

    /**
     * Copy a tuple to the current tuple position you must make sure the tuple
     * lengths must be equal
     *
     * @param fromNode
     *            the tuple being copied
     */
    public void edgeCopy(Edge fromEdge) {
        byte[] temparray = fromEdge.getEdgeByteArray();
        System.arraycopy(temparray, 0, data, edge_offset, edge_length);
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

    public void edgeInit(byte[] aedge, int offset) {
        data = aedge;
        edge_offset = offset;
        edge_length = max_size;
    }

    /**
     * Set an edge with the given edge length and offset
     *
     * @param fromEdge
     *            a byte array contains the edge
     * @param offset
     *            the offset of the edge ( =0 by default)
     *
     *            the length of the edge
     */
    public void edgeSet(byte[] fromEdge, int offset) {
        int length = max_size;
        System.arraycopy(fromEdge, offset, data, 0, length);
        edge_offset = 0;
        edge_length = length;
    }

    /**
     * get the length of a edge, call this method if you did not call setHdr ()
     * before
     *
     * @return length of this edge in bytes
     */
    public int getLength() {
        return edge_length;
    }

    /**
     * get the length of a edge, call this method if you did call setHdr ()
     * before
     *
     * @return size of this edge in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - edge_offset));
    }

    /**
     * get the offset of a edge
     *
     * @return offset of the edge in byte array
     */
    public int getOffset() {
        return edge_offset;
    }

    /**
     * Copy the edge byte array out
     *
     * @return byte[], a byte array contains the edge the length of byte[] =
     *         length of the edge
     */

    public byte[] getEdgeByteArray() {
        byte[] edgecopy = new byte[edge_length];
        System.arraycopy(data, edge_offset, edgecopy, 0, edge_length);
        return edgecopy;
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


    public String getLabel() throws IOException, FieldNumberOutOfBoundException{
        return getStrFld(1);
    }
    public int getLabel() throws IOException, FieldNumberOutOfBoundException{
        return getIntFld(2);
    }
    public NID getSource() throws IOException, FieldNumberOutOfBoundException {
        return (NID)getStrFld(3);
    }

    public NID getDestination() throws IOException, FieldNumberOutOfBoundException {
        return (NID)getStrFld(4);
    }





    public Edge setLabel(String val) throws IOException, FieldNumberOutOfBoundException {
        return (Edge)setStrFld(1, val);
    }
    public Edge setWeight(int val) throws IOException, FieldNumberOutOfBoundException {
        return (Edge)setStrFld(2, val);
    }
    public Edge setSource(NID sourceID) throws IOException, FieldNumberOutOfBoundException {
        return (Edge)setStrFld(3, val);
    }
    public Edge setDestination(NID destID) throws IOException, FieldNumberOutOfBoundException {
        return (Edge)setStrFld(4, val);
    }



    public void print() throws IOException, FieldNumberOutOfBoundException {
        System.out.println("Edge label: "+ getLabel());
        System.out.println("Edge Weight: ");
        System.out.print(desc.get(i)+"  ");
        }
    }





}
