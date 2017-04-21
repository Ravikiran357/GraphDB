package tests;

import java.io.IOException;

import btree.BTFileScan;
import btree.ConstructPageException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.LeafData;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import catalog.Utility;
import edgeheap.Edge;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.NID;
import global.SystemDefs;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.EdgeScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.NestedLoopExtended;
import iterator.NestedLoopExtendedEdge;
import iterator.NodeScan;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import nodeheap.Node;
import zIndex.DescriptorKey;

public class PathQuery {
	private String exp;
	private boolean OK = true;
	private boolean FAIL = false;
	private Iterator nextNodeIterator;
	private Iterator nextEdgeIterator;
	
	public PathQuery(String exp){
		this.exp = exp;
	}
	
	public void evaluate() throws ScanIteratorException, KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException, InvalidFrameNumberException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException{
		String[] n = exp.split("/");
		String element = n[0];
		element = element.trim();
		AttrType[] Ntypes = new AttrType[2];
		Ntypes[0] = new AttrType(AttrType.attrString);
		Ntypes[1] = new AttrType(AttrType.attrDesc);
		//node label
		if(element.startsWith("L")){
			String label = element.substring(1).trim();
			BTFileScan scan = SystemDefs.JavabaseDB.nodeLabelIndexFile.new_scan(new StringKey(label), new StringKey(label));
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect node data
				LeafData leafData = (LeafData) entry.data;
				NID nid = new NID();
				nid.copyRid(leafData.getData());
				//TODO call task 3
				Iterator tailNodes = task3Method(nid,n);
				
				
				Tuple t = null;
				try {
					
					while ((t = tailNodes.get_next()) != null) {
						
						t.print(Ntypes);

						//qcheck1.Check(t);
					}
				} catch (Exception e1) {
					System.err.println("" + e1);
					e1.printStackTrace();
					//status = FAIL;
				}
				try {
					tailNodes.close();
				} catch (Exception e1) {
					//status = FAIL;
					e1.printStackTrace();
				}
				
				
				entry = scan.get_next();
			}
			scan.DestroyBTreeFileScan();		}
		//node desc
		else if(element.startsWith("D")){
			Descriptor desc = Utility.convertToDescriptor(element.substring(1).trim());
			BTFileScan scan = SystemDefs.JavabaseDB.nodeDescriptorIndexFile.new_scan(new DescriptorKey(desc), new DescriptorKey(desc));
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect node data
				LeafData leafData = (LeafData) entry.data;
				NID nid = new NID();
				nid.copyRid(leafData.getData());
				//TODO call task 3
				Iterator tailNodes = task3Method(nid,n);
				
				entry = scan.get_next();
			}
			scan.DestroyBTreeFileScan();
		}
		//ignore
		/*
		for(String element : n){
			element = element.trim();
			//node label
			if(element.startsWith("L")){
				path.add(element.substring(1).trim());
			}
			//node desc
			else if(element.startsWith("D")){
				Descriptor desc = Utility.convertToDescriptor(element.substring(1).trim());
				path.add(desc);
			}
			//ignore
		}*/
		
		//
	}

	private Iterator task3Method(NID nid, String[] n) {
		
		
		// TODO Auto-generated method stub
		nextNodeIterator = null;
		nextEdgeIterator = null;
		//do nid join edge and edge join node
		int i = 0;
		while(i < n.length-1){
			doJoinNodeEdge(n[i++]);
			if(i>=n.length){
				break;
			}
			doJoinEdgeNode(n[i]);
			
		}
		
		return nextNodeIterator;
	}

	

	private void doJoinNodeEdge(String n) {
		boolean status = OK;
		// TODO Auto-generated method stub
		System.out.print("\n(Tests NodeScan, Projection, and Nested Loop Join)\n");

		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		
		outFilter[0].next = null;
		outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrString);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		String label = n.substring(1).trim();

		outFilter[0].operand2.string = label;
		
		outFilter[1] = null;
		
//		
//		CondExpr[] rightFilter = new CondExpr[2];
//		rightFilter[0] = new CondExpr();
//		
		//Query1_CondExpr(outFilter, rightFilter);
		Tuple t = new Tuple();
		Node n1;
		try {
			n1 = new Node();
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		AttrType[] Ntypes = new AttrType[2];
		Ntypes[0] = new AttrType(AttrType.attrString);
		Ntypes[1] = new AttrType(AttrType.attrDesc);
		
		// SOS
		short[] Nsizes = new short[2];
		Nsizes[0] = 44;
		//Nsizes[1] = 20;// first elt. is 30

		FldSpec[] Nprojection = new FldSpec[2];
		Nprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Nprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);//wrong
		
		CondExpr[] selects = new CondExpr[1];
		selects = null;
		Iterator am = null;
		if(nextNodeIterator == null){
		try {
			am = new NodeScan("nodeheapfile", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}
		}
		if(nextNodeIterator != null){
			try {
				am.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			am = nextNodeIterator;
		}


		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for Node");
			Runtime.getRuntime().exit(1);
		}
		AttrType[] jtype12 = new AttrType[2];
		jtype12[0] = new AttrType(AttrType.attrString);
		jtype12[1] = new AttrType(AttrType.attrDesc);

		AttrType[] Etypes = new AttrType[6];
		Etypes[0] = new AttrType(AttrType.attrString);//label
		Etypes[1] = new AttrType(AttrType.attrInteger);//pgidsource
		Etypes[2] = new AttrType(AttrType.attrInteger);//slotidsource
		Etypes[3] = new AttrType(AttrType.attrInteger);//pgiddest
		Etypes[4] = new AttrType(AttrType.attrInteger);//slotiddest
		Etypes[5] = new AttrType(AttrType.attrInteger);//weight

		short[] Esizes = new short[1];
		Esizes[0] = 44;
		FldSpec[] Eprojection = new FldSpec[6];
		Eprojection[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		Eprojection[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		Eprojection[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
		Eprojection[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
		Eprojection[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 5);
		Eprojection[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 6);
		
		AttrType[] JJtype = { new AttrType(AttrType.attrString),new AttrType(AttrType.attrDesc), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger)};

		short[] JJsize = new short[4];
		JJsize[0] = 44;
		JJsize[2] = 44;

//		FileScan am2 = null;
//		try {
//			am2 = new FileScan("edgeheapfile", Etypes, Esizes, (short) 6, (short) 6, Eprojection, null);
//		} catch (Exception e) {
//			status = FAIL;
//			System.err.println("" + e);
//		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}

//		FldSpec[] proj_list = new FldSpec[2];
//		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

		AttrType[] jtype = new AttrType[2];
		jtype[0] = new AttrType(AttrType.attrString);
		jtype[1] = new AttrType(AttrType.attrString);

		
		
		NestedLoopExtended inl = null;
		try {
			inl = new NestedLoopExtended(Ntypes, 2, Nsizes, Etypes, 6, Esizes, 10, am, "edgeSourceIndexFile", outFilter, null, Eprojection, 6);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		if(inl==null){
			System.out.print("inl null");
		}

		System.out.print("After nested loop join Node and Edge\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}
		
		//set Iterator
		nextEdgeIterator = inl;
//		Tuple t1 = null;
//		try {
//			
//			while ((t1 = inl.get_next()) != null) {
//				
//				t1.print(Etypes);
//
//				//qcheck1.Check(t);
//			}
//		} catch (Exception e1) {
//			System.err.println("" + e1);
//			e1.printStackTrace();
//			//status = FAIL;
//		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}

		//qcheck1.report(1);
		try {
			//inl.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println("\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}

		
	}
	
	private void doJoinEdgeNode(String n) {
		boolean status = OK;

		// TODO Auto-generated method stub
		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
	
		outFilter[0].next = null;
		outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
		outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		
		outFilter[1] = null;
		
		CondExpr[] rightFilter = new CondExpr[2];
		rightFilter[0] = new CondExpr();
		
		if(n.startsWith("D")){
			String desc_string = n.substring(1).trim();
				
		rightFilter[0].next = null;
		rightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		rightFilter[0].type2 = new AttrType(AttrType.attrDesc);
		rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		Descriptor desc1 = new Descriptor();
		desc1 = Utility.convertToDescriptor(n.substring(1).trim());
		rightFilter[0].operand2.desc = desc1;
		
		rightFilter[1] = null;
		//outFilter[0].updateDesc();
		rightFilter[0].updateDesc();
		}
		if(n.startsWith("L")){
			String label = n.substring(1).trim();
				
		rightFilter[0].next = null;
		rightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		rightFilter[0].type2 = new AttrType(AttrType.attrString);
		rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		
		rightFilter[0].operand2.string = label;
		
		rightFilter[1] = null;
		}
		//Query3_CondExpr(outFilter, rightFilter);
		
		Tuple t = new Tuple();
		Edge e;
		try {
			e = new Edge();
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		AttrType[] Etypes = new AttrType[6];
		Etypes[0] = new AttrType(AttrType.attrString);//label
		Etypes[1] = new AttrType(AttrType.attrInteger);//pgidsource
		Etypes[2] = new AttrType(AttrType.attrInteger);//slotidsource
		Etypes[3] = new AttrType(AttrType.attrInteger);//pgiddest
		Etypes[4] = new AttrType(AttrType.attrInteger);//slotiddest
		Etypes[5] = new AttrType(AttrType.attrInteger);//weight

		short[] Esizes = new short[1];
		Esizes[0] = 44;
		FldSpec[] Eprojection = new FldSpec[6];
		Eprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Eprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Eprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Eprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		Eprojection[4] = new FldSpec(new RelSpec(RelSpec.outer), 5);
		Eprojection[5] = new FldSpec(new RelSpec(RelSpec.outer), 6);
		CondExpr[] selects = new CondExpr[1];
		selects = null; 

		Iterator am = null;
		try {
			am = new EdgeScan("edgeheapfile", Etypes, Esizes, (short) 6, (short) 6, Eprojection, null);
		} catch (Exception e1) {
			status = FAIL;
			System.err.println("" + e1);
		}
		if(nextEdgeIterator!=null){
			try {
				am.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
			am = nextEdgeIterator;
		}
		

		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for Edge");
			Runtime.getRuntime().exit(1);
		}
		AttrType[] Ntypes = new AttrType[2];
		Ntypes[0] = new AttrType(AttrType.attrString);
		Ntypes[1] = new AttrType(AttrType.attrDesc);
		
		// SOS
		short[] Nsizes = new short[2];
		Nsizes[0] = 44;
		//Nsizes[1] = 20;// first elt. is 30

		AttrType[] jtype12 = new AttrType[2];
		jtype12[0] = new AttrType(AttrType.attrString);
		jtype12[1] = new AttrType(AttrType.attrDesc);

				
		AttrType[] JJtype = { new AttrType(AttrType.attrString),new AttrType(AttrType.attrDesc)};

		short[] JJsize = new short[1];
		JJsize[0] = 44;
		

//		FileScan am2 = null;
//		try {
//			am2 = new FileScan("edgeheapfile", Etypes, Esizes, (short) 6, (short) 6, Eprojection, null);
//		} catch (Exception e) {
//			status = FAIL;
//			System.err.println("" + e);
//		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}

//		
		AttrType[] jtype = new AttrType[2];
		jtype[0] = new AttrType(AttrType.attrString);
		jtype[1] = new AttrType(AttrType.attrDesc);

		
		
		FldSpec[] proj1 = new FldSpec[2];
		proj1[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		proj1[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		
		NestedLoopExtendedEdge inl = null;
		try {
			inl = new NestedLoopExtendedEdge(Etypes, 6, Esizes, Ntypes, 2, Nsizes, 10, am, "nodeLabelIndexFile", outFilter, rightFilter, proj1, 2);
		} catch (Exception e1) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e1);
			e1.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		if(inl==null){
			System.out.print("inl null");
		}

		System.out.print("After nested loop join Edge and Node\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}
				
		nextNodeIterator = inl;
		
		if (status != OK) {
			// bail out
			System.err.println("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}

		//qcheck1.report(1);
		try {
			//inl.close();
		} catch (Exception e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		System.out.println("\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
		
	}
}
