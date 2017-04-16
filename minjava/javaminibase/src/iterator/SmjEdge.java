package iterator;

import java.io.IOException;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.InvalidTupleSizeException;
import global.AttrOperator;
import global.AttrType;
import global.EID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;

public class SmjEdge {
	public static Heapfile joinHeapfile;
	private void Query1_CondExpr(CondExpr[] expr) {

		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

		expr[1] = null;
	}
	
	private void printEdgesInHeap() throws InvalidTupleSizeException, IOException, 
	FieldNumberOutOfBoundException {
		EScan eScan = new EScan(SystemDefs.JavabaseDB.edgeHeapfile);
	    EID eid = new EID();
	    System.out.println("Printing edge data using edge heap file");
	    Edge edge = eScan.getNext(eid);
	    while(edge != null) {
	    	// Print edge details
	    	edge.print();
	    	edge = eScan.getNext(eid);
	    }
	    eScan.closescan();
	}
	
//	public String returnJoinedFile(){
//		return "joinheapfile";
//	}

	
	public SmjEdge(String filename1, String filename2) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		printEdgesInHeap();
		joinHeapfile = new Heapfile("joinheapfile");
		AttrType[] attrs = new AttrType[6];
        attrs[0] = new AttrType(AttrType.attrString);
        attrs[1] = new AttrType(AttrType.attrInteger); //source pg no.
        attrs[2] = new AttrType(AttrType.attrInteger);//source slot no.
        attrs[3] = new AttrType(AttrType.attrInteger);//dest pg no.
        attrs[4] = new AttrType(AttrType.attrInteger);//dest slot no.
        attrs[5] = new AttrType(AttrType.attrInteger);
		short[] attrSize = new short[1];
		attrSize[0] = 44;
		RelSpec rel_out = new RelSpec(RelSpec.outer);	
		RelSpec rel_in = new  RelSpec(RelSpec.innerRel);
		
		FldSpec[] out_projlist = new FldSpec[6];
		out_projlist[0] = new FldSpec(rel_out, 1);
		out_projlist[1] = new FldSpec(rel_out, 2);
		out_projlist[2] = new FldSpec(rel_out, 3);
		out_projlist[3] = new FldSpec(rel_out, 4);
		out_projlist[4] = new FldSpec(rel_out, 5);
		out_projlist[5] = new FldSpec(rel_out, 6);
		
		FileScan out_fscan = new FileScan(filename1, attrs, attrSize, (short) 6, 6, out_projlist, null);
		FldSpec[] in_projlist = new FldSpec[6];
		in_projlist[0] = new FldSpec(rel_in, 1);
		in_projlist[1] = new FldSpec(rel_in, 2);
		in_projlist[2] = new FldSpec(rel_in, 3);
		in_projlist[3] = new FldSpec(rel_in, 4);
		in_projlist[4] = new FldSpec(rel_in, 5);
		in_projlist[5] = new FldSpec(rel_in, 6);
		FileScan in_fscan = new FileScan(filename2, attrs, attrSize, (short) 6, 6, out_projlist, null);

		
		FldSpec[] projlist = new FldSpec[6];
		projlist[0] = new FldSpec(rel_out, 1);
		projlist[1] = new FldSpec(rel_out, 4);
		projlist[2] = new FldSpec(rel_out, 5);
		projlist[3] = new FldSpec(rel_in, 1);
		projlist[4] = new FldSpec(rel_in, 2);
		projlist[5] = new FldSpec(rel_in, 3);
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;
		
		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		Query1_CondExpr(outFilter);
		
		try {
			sm = new SortMerge(
					attrs, 6, attrSize, attrs, 6, attrSize, 
					5, 4, 3, 4,
					25, out_fscan, in_fscan, false, false, ascending,
					null, projlist, 6, -1.0, null);
		} catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***");
			System.err.println("" + e);
			e.printStackTrace();
		}
		Tuple t = sm.get_next();
		int counter =0;
		while (t != null) {
			try {
				System.out.println("Counter = "+ counter++);
				String outval = t.getStrFld(1);
				int outval1 = t.getIntFld(2);
				int outval2 = t.getIntFld(3);
				String outval3 = t.getStrFld(4);
				int outval4 = t.getIntFld(5);
				int outval5 = t.getIntFld(6);
				System.out.println(outval + " " + outval1 + " " + outval2 + " " + outval3 + " " + outval4 + " " + outval5);
				System.out.println("!!!!!!");
			} catch (Exception e) {
				e.printStackTrace();
			}
			joinHeapfile.insertRecord(t.getTupleByteArray());
			t = sm.get_next();
			
			//returnJoinedFile();
		}
	}
	
}
