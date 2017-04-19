package tests;

import java.io.IOException;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapfile;

import global.AttrType;
import global.EID;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.SmjEdge;
import iterator.UnknowAttrType;


public class TriangleQuery {
	public String label1;
	public String label2;
	public String label3;
	
	TriangleQuery(){
		label1 = "1_2";
		label2 = "2_3";
		label3 = "3_1";
		
	}

	private Tuple setHdr(Tuple t) throws InvalidTypeException, InvalidTupleSizeException, IOException {
		AttrType[] attrs = new AttrType[6];
        short[] str_sizes = new short[1];
        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrInteger); //source pg no.
        attrs[2] = new AttrType(AttrType.attrInteger); //source slot no.
        attrs[3] = new AttrType(AttrType.attrInteger); //dest pg no.
        attrs[4] = new AttrType(AttrType.attrInteger); //dest slot no.
        attrs[5] = new AttrType(AttrType.attrInteger);
        str_sizes[0] = (short)44;
        t.setHdr((short)6, attrs, str_sizes);
        return t;
	}
	
	private Tuple setTriHdr(Tuple t) throws InvalidTypeException, InvalidTupleSizeException, IOException {
		AttrType[] attrs = new AttrType[18];
        short[] str_sizes = new short[3];
        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrInteger); //source pg no.
        attrs[2] = new AttrType(AttrType.attrInteger); //source slot no.
        attrs[3] = new AttrType(AttrType.attrInteger); //dest pg no.
        attrs[4] = new AttrType(AttrType.attrInteger); //dest slot no.
        attrs[5] = new AttrType(AttrType.attrInteger);
        attrs[6] = new AttrType(AttrType.attrString);
        attrs[7] = new AttrType(AttrType.attrInteger); //source pg no.
        attrs[8] = new AttrType(AttrType.attrInteger); //source slot no.
        attrs[9] = new AttrType(AttrType.attrInteger); //dest pg no.
        attrs[10] = new AttrType(AttrType.attrInteger); //dest slot no.
        attrs[11] = new AttrType(AttrType.attrInteger);
        attrs[12] = new AttrType(AttrType.attrString);
        attrs[13] = new AttrType(AttrType.attrInteger); //source pg no.
        attrs[14] = new AttrType(AttrType.attrInteger); //source slot no.
        attrs[15] = new AttrType(AttrType.attrInteger); //dest pg no.
        attrs[16] = new AttrType(AttrType.attrInteger); //dest slot no.
        attrs[17] = new AttrType(AttrType.attrInteger);
        str_sizes[0] = (short)44;
        str_sizes[1] = (short)44;
        str_sizes[2] = (short)44;
        t.setHdr((short)18, attrs, str_sizes);
        return t;
	}
	
	private void filterTupleLabels(EdgeHeapfile hf,  String label, String outheapfile) throws 
			HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidTupleSizeException,
			InvalidSlotNumberException, SpaceNotAvailableException,	FieldNumberOutOfBoundException,
			edgeheap.InvalidTupleSizeException, InvalidTypeException {
		Heapfile outhf = new Heapfile(outheapfile);
		EScan fscan = new EScan(hf);
		EID eid = new EID();
		Edge edge = fscan.getNext(eid);
        while(edge != null){
            Tuple t = new Tuple(edge.getTupleByteArray(), 0, edge.getLength());
            t = setHdr(t);
            String tupleLabel = t.getStrFld(1); 
            if(tupleLabel.equals(label)){
            	//Add tuples to the new heapfile
                outhf.insertRecord(t.getTupleByteArray());
            }
            edge = fscan.getNext(eid);
        }
        fscan.closescan();
	}
	
	
	private void filterTupleWeights(EdgeHeapfile hf,  int max_weight, String outheapfile) throws 
		HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidTupleSizeException,
		InvalidSlotNumberException, SpaceNotAvailableException,	FieldNumberOutOfBoundException,
		edgeheap.InvalidTupleSizeException, InvalidTypeException {
			Heapfile outhf = new Heapfile(outheapfile);
			EScan fscan = new EScan(hf);
			EID eid = new EID();
			Edge edge = fscan.getNext(eid);
			while(edge != null){
			    Tuple t = new Tuple(edge.getTupleByteArray(), 0, edge.getWeight());
			    t = setHdr(t);
			    int tupleWeight = t.getIntFld(6); 
			    if(tupleWeight <= max_weight){
			    	//Add tuples to the new heapfile
			        outhf.insertRecord(t.getTupleByteArray());
			    }
			    edge = fscan.getNext(eid);
			}
			fscan.closescan();
			}


	private void filterTupleByNID(String heapfile, String resheapfile) throws 
		HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidTupleSizeException,
		InvalidSlotNumberException, SpaceNotAvailableException,	FieldNumberOutOfBoundException,
		edgeheap.InvalidTupleSizeException, InvalidTypeException {
		Heapfile hf = new Heapfile(heapfile);
		Heapfile reshf = new Heapfile(resheapfile);
		Scan fscan = new Scan(hf);
		RID rid = new RID();
		Tuple tuple = fscan.getNext(rid);
		while(tuple != null){
		    tuple = setTriHdr(tuple);
		    // Checking common NID of 1st and 3rd edge
		    if((tuple.getIntFld(2) == tuple.getIntFld(16))
		    && (tuple.getIntFld(3) == tuple.getIntFld(17))) {
		    	//Add tuples to the final heapfile
		    	reshf.insertRecord(tuple.getTupleByteArray());
		    }
		    tuple = fscan.getNext(rid);
		}
		fscan.closescan();
}
	
	public void printTuplesInRelation(String heapfilename) throws FieldNumberOutOfBoundException, 
		IOException, InvalidTupleSizeException, HFException, HFBufMgrException, 
		HFDiskMgrException, InvalidTypeException{
		Heapfile hf = new Heapfile(heapfilename);
		Scan fscan = new Scan(hf);
		RID rid = new RID();
		Tuple t = fscan.getNext(rid);
        while(t != null){
            t = setHdr(t);
            System.out.println(t.getStrFld(1));
            t = fscan.getNext(rid);
        }
        fscan.closescan();
	}

	public void startTriangleQuery(String[] args, String[] values) throws UnknowAttrType, LowMemException, JoinsException, Exception{
		EdgeHeapfile hf = SystemDefs.JavabaseDB.edgeHeapfile;
		int joinOperationType = 0;

		//From the edge relation filter label1 from R relation and label2 from S relation
		String rheapfile = "filterlabels1";
		String sheapfile = "filterlabels2";
		
		if(args[0].equals("w")){
			filterTupleWeights(hf, Integer.parseInt(values[0]), rheapfile);
		}else if(args[0].equals("l")){
			filterTupleLabels(hf, values[0], rheapfile);
		}
		
		if(args[1].equals("w")){
			filterTupleWeights(hf, Integer.parseInt(values[1]), sheapfile);
		}else if(args[1].equals("l")){
			filterTupleLabels(hf, values[1], sheapfile);
		}

		String joinheapfile1 = "joinheapfile1";
		SmjEdge smj1 = new SmjEdge();
		smj1.joinOperation(rheapfile, sheapfile, joinheapfile1, joinOperationType, true);

		//Pass the already joined heapfile and the file filtered on label3 as input to smj
		joinOperationType = 1;
		String sheapfile_s = "filterlabels3";
		
		if(args[2].equals("w")){
			filterTupleWeights(hf, Integer.parseInt(values[2]), sheapfile_s);
		}else if(args[2].equals("l")){
			filterTupleLabels(hf, values[2], sheapfile_s);
		}
		
		String joinheapfile2 = "joinheapfile2";
		SmjEdge smj2 = new SmjEdge();
		smj2.joinOperation(joinheapfile1, sheapfile_s, joinheapfile2, joinOperationType, true);
		
		//Filter by checking NID of 3rd edge and 1st edge
		String resFileName = "resultTriangels";
		filterTupleByNID(joinheapfile2, resFileName);

		//Printing the results
		smj2.printTuplesInRelation(resFileName, 1);
	}
}
