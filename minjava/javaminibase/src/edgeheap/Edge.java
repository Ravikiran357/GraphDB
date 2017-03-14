/* File Edge.java */

package edgeheap;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import java.io.*;
import java.lang.*;
import global.*;

public class Edge extends Tuple {

    /**
     * Maximum size of any tuple
     */
    public static final int max_size = 82;

    /**
     * Class constructor Creat a new tuple with length = max_size,tuple offset =
     * 0.
     * @throws IOException 
     * @throws InvalidTupleSizeException 
     * @throws InvalidTypeException 
     */

    public Edge() throws InvalidTypeException, InvalidTupleSizeException, IOException {
        // Creat a new tuple
    	super(max_size);
        setEdgeHdr();
    }

    /**
     * Constructor
     *
     *  anode
     *            a byte array which contains the tuple
     * @param offset
     *            the offset of the tuple in the byte array
     *
     *            the length of the tuple
     * @throws IOException 
     * @throws InvalidTupleSizeException 
     * @throws InvalidTypeException 
     */

    public Edge(byte[] aedge, int offset) throws InvalidTypeException, InvalidTupleSizeException, IOException {
    	super(aedge, offset, max_size);
        setEdgeHdr();
        // fldCnt = getShortValue(offset, data);
    }
    
    public Edge(byte[] aedge, int offset, int size) throws InvalidTypeException, InvalidTupleSizeException, IOException {
    	super(aedge, offset, size);
        if(size == 82) setEdgeHdr();
        // fldCnt = getShortValue(offset, data);
    }
    
    private void setEdgeHdr() throws InvalidTypeException, InvalidTupleSizeException, IOException{
        AttrType[] attrs = new AttrType[6];
        short[] str_sizes = new short[1];
        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrInteger); //source pg no.
        attrs[2] = new AttrType(AttrType.attrInteger);//source slot no.
        attrs[3] = new AttrType(AttrType.attrInteger);//dest pg no.
        attrs[4] = new AttrType(AttrType.attrInteger);//dest slot no.
        attrs[5] = new AttrType(AttrType.attrInteger);
        str_sizes[0] = (short)44;
        this.setHdr((short)6, attrs, str_sizes);
    }

    /**
     * Constructor(used as tuple copy)
     *
     *  fromNode
     *            a byte array which contains the tuple
     *
     */
    public Edge(Edge fromEdge) {
    	super(fromEdge);
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
     *  fromNode
     *            the tuple being copied
     */
    public void edgeCopy(Edge fromEdge) {
    	tupleCopy(fromEdge);
    }

    /**
     * This is used when you don't want to use the constructor
     *
     *  anode
     *            a byte array which contains the tuple
     * @param offset
     *            the offset of the tuple in the byte array
     *
     *            the length of the tuple
     */

    public void edgeInit(byte[] aedge, int offset) {
    	tupleInit(aedge, offset, max_size);
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
        tupleSet(fromEdge, offset, max_size);
    }


    /**
     * Copy the edge byte array out
     *
     * @return byte[], a byte array contains the edge the length of byte[] =
     *         length of the edge
     */

    public byte[] getEdgeByteArray() {
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


    public String getLabel() throws IOException, heap.FieldNumberOutOfBoundException{
        return getStrFld(1);
    }
    
    public NID getSource() throws IOException, heap.FieldNumberOutOfBoundException {
         int pid = getIntFld(2);
         int slotno = getIntFld(3);
         return new NID(new PageId(pid), slotno);
    }

    public NID getDestination() throws IOException, heap.FieldNumberOutOfBoundException {
        int pid = getIntFld(4);
        int slotno = getIntFld(5);
        return new NID(new PageId(pid), slotno);    }
    
    public int getWeight() throws IOException, heap.FieldNumberOutOfBoundException{
        return getIntFld(6);
    }



	private NID getNIDFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
		NID val = new NID();
		RID rid = getRIDFld(fldNo);
		val.pageNo = rid.pageNo;
		val.slotNo = rid.slotNo;
		return val;
	}


    public Edge setLabel(String val) throws IOException, heap.FieldNumberOutOfBoundException {
        return (Edge)setStrFld(1, val);
    }
    public Edge setSource(NID sourceID) throws IOException, heap.FieldNumberOutOfBoundException {
    	setIntFld(2, sourceID.pageNo.pid);
    	return (Edge)setIntFld(3,sourceID.slotNo);
    }
    public Edge setDestination(NID destID) throws IOException, heap.FieldNumberOutOfBoundException {
    	setIntFld(4, destID.pageNo.pid);
    	return (Edge)setIntFld(5,destID.slotNo);    }
    public Edge setWeight(int val) throws IOException, heap.FieldNumberOutOfBoundException {
        return (Edge)setIntFld(6, val);
    }



    public void print() throws IOException, heap.FieldNumberOutOfBoundException {
        System.out.println("label is "+ getLabel());
        System.out.println("Weight is : "+ getWeight());
        NID source = getSource();
        NID dest = getDestination();
        System.out.println("Source pid: " + source.pageNo + ", slotnum "+ source.slotNo);
        System.out.println("Dest pid: " + dest.pageNo + ", slotnum "+ dest.slotNo);
        System.out.println("\n");
    }
}
