package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import btree.BTFileScan;
import btree.BTreeFile;
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
import diskmgr.GraphDB;
import edgeheap.EScan;
import edgeheap.Edge;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.NID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import iterator.FldSpec;
import iterator.RelSpec;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.InvalidTupleSizeException;
import nodeheap.NScan;
import nodeheap.Node;
import zIndex.ZTreeFile;


public class NodeQuery {

	private final static boolean OK = true;
	private final static boolean FAIL = false;
	private GraphDB db;
	
	private Descriptor convertToDescriptor(String desc) {
		Descriptor descriptor = new Descriptor();
		String [] val = desc.split(",");
		descriptor.set(Integer.parseInt(val[0]), Integer.parseInt(val[1]), Integer.parseInt(val[2]), 
				Integer.parseInt(val[3]), Integer.parseInt(val[4]));
		return descriptor;
	}
	
	private void printNodesInHeap() throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {	
		NScan nScan = new NScan(db.nodeHeapfile);
        NID nid = new NID();
        System.out.println("Printing node data using node heap file");
        Node node = nScan.getNext(nid);
        while(node != null){
        	// Print Node details
        	node.print();
        	node = nScan.getNext(nid);
        }
        nScan.closescan();
	}
	
	private void printNodeLabels(int index) throws InvalidSlotNumberException, 
		HFException, HFDiskMgrException, HFBufMgrException, Exception {	
		List<Node> nodeList = new ArrayList<Node>();
		if (index == 1) {
			System.out.println("Printing node labels in alphabetical order using index file");
			NID nid = new NID();
			BTreeFile indexFile = db.nodeLabelIndexFile;
			BTFileScan scan = indexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect node data
				LeafData leafData = (LeafData) entry.data;
				nid.copyRid(leafData.getData());
				Node node = db.nodeHeapfile.getNode(nid);
				nodeList.add(node);
				entry = scan.get_next();
			}
			scan.DestroyBTreeFileScan();
		} else {
			System.out.println("Printing node labels in alphabetical order using node heap file");
			NScan nScan = new NScan(db.nodeHeapfile);
	        NID nid = new NID();
	        Node node = nScan.getNext(nid);
	        while(node != null){
            	// Collect node data
	        	nodeList.add(node);
            	node = nScan.getNext(nid);
	        }
	        nScan.closescan();
	        Collections.sort(nodeList, new Comparator<Node>() {
	            public int compare(Node n1,Node n2) {
	            	try {
						return n1.getLabel().compareTo(n2.getLabel());
					} catch (FieldNumberOutOfBoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return 0;
	            }
	        });
		}
        for (Node node : nodeList) {
			node.print();
		}
	}

	private void printNodeDataFromTarget(int index, String desc) throws InvalidSlotNumberException, HFException, 
		HFDiskMgrException, HFBufMgrException, Exception {
		Descriptor descriptor = convertToDescriptor(desc);		
		double dist;
		if (index == 1) {
			System.out.println("Printing node data in order using index file");
			ZTreeFile indexFile = db.nodeDescriptorIndexFile;
			List<NID> nidList = indexFile.zTreeFileScan();
			List<NID> tempList;
			TreeMap<Double,List<NID>> hash = new TreeMap<Double,List<NID>>();
        	// Make sorted list of NID
			for (NID nid : nidList) {
				Node node = db.nodeHeapfile.getNode(nid);
				dist = node.getDesc().distance(descriptor);
				if (hash.containsKey(dist)) {
					tempList = hash.get(dist);
				} else {
					tempList = new ArrayList<NID>();
				}
				tempList.add(nid);
				hash.put(dist, tempList);
			}
			
			for (Map.Entry<Double, List<NID>> entry : hash.entrySet()) {
				nidList = entry.getValue();
				for (NID nid : nidList) {
					Node node = db.nodeHeapfile.getNode(nid);
					node.print();
				}
			}
		} else {
			System.out.println("Printing node data in order using node heap file");
			NScan nScan = new NScan(db.nodeHeapfile);
	        NID nid = new NID();
	        TreeMap<Double,List<Node>> hash = new TreeMap<Double,List<Node>>();
	        List<Node> tempList;
	        Node node = nScan.getNext(nid);
	        while(node != null){
	        	// Make sorted list of Nodes
	        	dist = node.getDesc().distance(descriptor);
	        	if (hash.containsKey(dist)) {
					tempList = hash.get(dist);
				} else {
					tempList = new ArrayList<Node>();
				}
	        	tempList.add(node);
	        	hash.put(dist, tempList);
	        	node = nScan.getNext(nid);
	        }
	        nScan.closescan();
	        List<Node> nidList = new ArrayList<Node>();
	        for (Map.Entry<Double, List<Node>> entry : hash.entrySet()) {
				nidList = entry.getValue();
				for (Node each_node : nidList) {
					each_node.print();
				}
			}
		}
	}
	
	public void printNodeLabelFromTargetDistance(int index, String desc, int dist) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		//Sort sort = null;
		Node node = null;
		Descriptor descriptor = convertToDescriptor(desc);
		
		if (index == 1) {
			ZTreeFile indexFile = db.nodeDescriptorIndexFile;
			List<NID> nidList = indexFile.zFileRangeScan(descriptor, dist);
			for(NID nid : nidList){
				node = db.nodeHeapfile.getNode(nid);
				System.out.println(node.getLabel());
			}
		} else {
			try {
				NScan nScan = new NScan(db.nodeHeapfile);
				NID nid = new NID();
				// create an iterator by open a file scan
				node = nScan.getNext(nid);
				while(node != null) {
					if(descriptor.distance(node.getDesc()) <= dist){
						System.out.println(node.getLabel());
					}
					node = nScan.getNext(nid);
				}
		        nScan.closescan();
			} catch (Exception e) {
				System.out.println("ERROR: In Task 14, Qtype 4.\n");
				e.printStackTrace();
			}
		}
	}
	
	private void printNodesWithLabel(int index, String label) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception{
		if (index == 1) {
			System.out.println("Printing node information and assosciated edges for nodes with same label using index file");
			BTreeFile indexFile = db.nodeLabelIndexFile;
			//low == high for doing exact match in btree.
			BTFileScan scan = indexFile.new_scan(new StringKey(label),new StringKey(label));
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect node data
				LeafData leafData = (LeafData) entry.data;
				NID nid = new NID();
				nid.copyRid(leafData.getData());
				Node node = db.nodeHeapfile.getNode(nid);
				printNodeAndEdgesContainingNode(node, nid);
				entry = scan.get_next();
			}
			scan.DestroyBTreeFileScan();
		} else {
			System.out.println("Printing node information and assosciated edges for nodes with same label using node heap file");
			NScan nScan = new NScan(db.nodeHeapfile);
	        NID nid = new NID();
	        Node node = nScan.getNext(nid);
	        while(node != null){
            	// Collect node data
	        	if(node.getLabel().equals(label)){
	        		printNodeAndEdgesContainingNode(node, nid);
	        	}
            	node = nScan.getNext(nid);
	        }
	        nScan.closescan();
		}
	}
	
	private void printNodeAndEdgesContainingNode(Node node, NID nid) throws edgeheap.InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {
    	EScan eScan = new EScan(SystemDefs.JavabaseDB.edgeHeapfile);
    	EID eid = new EID();
        boolean done = true;
        List<Edge> outgoingEdges = new ArrayList<Edge>();
        List<Edge> incomingEdges = new ArrayList<Edge>();
        while(done){
            Edge e  = eScan.getNext(eid);
            if(e == null){
                done = false;
                eScan.closescan();
                break;
            }

            if(e.getSource().equals(nid)){
            	outgoingEdges.add(e);
            }
            else if(e.getDestination().equals(nid)){
            	incomingEdges.add(e);
            }
        }
        
        node.print();
        System.out.print("Incoming Edges : \n");
        for(Edge e : incomingEdges) e.print();;
        System.out.print("\nOutgoing Edges : ");
        for(Edge e : outgoingEdges) e.print();;
        System.out.print("\n\n");
	}

	private void printNodesFromTargetDistance(int index, String desc, int dist) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		//Sort sort = null;
		Node node = null;
		Descriptor descriptor = convertToDescriptor(desc);
	
		if (index == 1) {
			ZTreeFile indexFile = db.nodeDescriptorIndexFile;
			List<NID> nidList = indexFile.zFileRangeScan(descriptor, dist);
			for(NID nid : nidList){
				node = db.nodeHeapfile.getNode(nid);
				System.out.println(node.getLabel());
		}		}
		else {
		try {
			NScan nScan = new NScan(db.nodeHeapfile);
			NID nid = new NID();
			// create an iterator by open a file scan
			node = nScan.getNext(nid);
			while(node != null) {
				if(descriptor.distance(node.getDesc()) <= dist){
					printNodeAndEdgesContainingNode(node,nid);
				}
				node = nScan.getNext(nid);
			}
			nScan.closescan();
		} catch (Exception e) {
			System.out.println("ERROR: In Task 14, Qtype 4.\n");
			e.printStackTrace();
		}
	}
}		

	public boolean evaluate(int qType, int index, String []args) {
		boolean status = OK;
		if (args.length > 0) {
			try {
				this.db = SystemDefs.JavabaseDB;
				String descriptor;
	
				switch(qType) {
					case 0: this.printNodesInHeap();
							break;
					case 1: this.printNodeLabels(index);
							break;
					case 2: 
							
							descriptor = args[0]; // expecting descriptor as CSV values
							this.printNodeDataFromTarget(index, descriptor);
							break;
					case 3: descriptor = args[0]; // expecting descriptor as CSV values
							int dist = Integer.parseInt(args[1]);
							this.printNodeLabelFromTargetDistance(index, descriptor, dist);
							break;
					case 4:String label = args[0]; //expect a label
						   this.printNodesWithLabel(index,label);
						   break;
					case 5:descriptor = args[0]; // expecting descriptor as CSV values
						   dist = Integer.parseInt(args[1]);
						   this.printNodesFromTargetDistance(index, descriptor, dist);
					default:
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Failed node query");
			}
		} else {
			status = FAIL;
			System.out.println("No inputs given\n");
		}
		return status;
	}
}

