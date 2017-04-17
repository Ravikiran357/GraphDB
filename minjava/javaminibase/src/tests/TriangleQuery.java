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
	
	public void filterTupleLabels(EdgeHeapfile hf,  String label, String outheapfile) throws 
			HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidTupleSizeException,
			InvalidSlotNumberException, SpaceNotAvailableException,	FieldNumberOutOfBoundException,
			edgeheap.InvalidTupleSizeException, InvalidTypeException {
		Heapfile outhf = new Heapfile(outheapfile);
		EScan fscan = new EScan(hf);
		EID rid = new EID();
		Edge edge = fscan.getNext(rid);
        while(edge != null){
            Tuple t = new Tuple(edge.getTupleByteArray(), 0, edge.getLength());
            t = setHdr(t);
            if(t.getStrFld(1).equals(label)){
            	//Add tuples to the new heapfile
                outhf.insertRecord(t.getTupleByteArray());
            }
            edge = fscan.getNext(rid);
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

	public void startTriangleQuery() throws UnknowAttrType, LowMemException, JoinsException, Exception{
		EdgeHeapfile hf = SystemDefs.JavabaseDB.edgeHeapfile;
		int joinOperationType = 0;

		//From the edge relation filter label1 from R relation and label2 from S relation
		String rheapfile = "filterlabels1";
		String sheapfile = "filterlabels2";
		filterTupleLabels(hf, label1, rheapfile);
		filterTupleLabels(hf, label2, sheapfile);
		String joinheapfile1 = "joinheapfile1";
		SmjEdge smj1 = new SmjEdge();
		smj1.joinOperation(rheapfile, sheapfile, joinheapfile1, joinOperationType, true);
		
		//Pass the already joined heapfile and the file filtered on label3 as input to smj
		joinOperationType = 1;
		String sheapfile_s = "filterlabels3";
		filterTupleLabels(hf, label3, sheapfile_s);
		String joinheapfile2 = "joinheapfile2";
		SmjEdge smj2 = new SmjEdge();
		smj2.joinOperation(joinheapfile1, sheapfile_s, joinheapfile2, joinOperationType, true);
	}
}
