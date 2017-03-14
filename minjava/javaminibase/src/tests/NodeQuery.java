
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;

import java.io.IOException;
import java.util.*;

import zIndex.ZTreeFile;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.KeyDataEntry;
import btree.LeafData;

import global.AttrType;
import global.Descriptor;
import global.NID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import diskmgr.GraphDB;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.InvalidTupleSizeException;
import nodeheap.NScan;
import nodeheap.Node;


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
			BTreeFile indexFile = db.nodeLabelIndexFile;
			BTFileScan scan = indexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect node data
				LeafData leafData = (LeafData) entry.data;
				Node node = db.nodeHeapfile.getNode((NID) leafData.getData());
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
	
	public void printNodeLabelFromTargetDistance(int index, String desc, int dist) {
		List<Node> nidList = new ArrayList<Node>();
		Sort sort = null;
		Node node = null;
		Descriptor descriptor = convertToDescriptor(desc);
		
		if (index == 1) {
			//TODO: with index
		} else {
			FileScan fscan = null;
			short numFlds = 2;
	        
			short[] str_sizes = new short[1];
	        str_sizes[0] = (short) 44;
	        
	        AttrType[] attrs = new AttrType[2];
	        attrs[0] = new AttrType(AttrType.attrString);
	        attrs[1] = new AttrType(AttrType.attrDesc);
			
			TupleOrder[] order = new TupleOrder[2];
			order[0] = new TupleOrder(TupleOrder.Ascending);
			order[1] = new TupleOrder(TupleOrder.Descending);
	        
			FldSpec[] projlist = new FldSpec[2];
			RelSpec rel = new RelSpec(RelSpec.outer);
			projlist[0] = new FldSpec(rel, 1);
			projlist[1] = new FldSpec(rel, 2);
	
			try {
				// create an iterator by open a file scan
				fscan = new FileScan("14_Q4.in", attrs, str_sizes, (short) numFlds, 2, 
						projlist, null);
				Tuple t = new Tuple();
				t.setHdr(numFlds, attrs, str_sizes);
				sort = new Sort(attrs, numFlds, str_sizes, fscan, 1, order[0], 
						0, 12, (double) dist, descriptor);
				node = (Node) sort.get_next();
				while (node != null) {
					// return node labels and distance
					nidList.add(node);
					node = (Node) sort.get_next();
				}
				sort.close();
				fscan.close();
			} catch (Exception e) {
				System.out.println("ERROR: In Task 14, Qtype 4.\n");
				e.printStackTrace();
			}
		}
	}

	public boolean evaluate(String []args) {
		boolean status = OK;
		if (args.length > 0) {
			try {
				String graphDBName = args[0];
				String numBuf = args[1];
				int qType = Integer.parseInt(args[2]);
				int index = Integer.parseInt(args[3]);
				this.db = SystemDefs.JavabaseDB;
				String descriptor;
	
				switch(qType) {
					case 0: this.printNodesInHeap();
							break;
					case 1: this.printNodeLabels(index);
							break;
					case 2: descriptor = args[4]; // expecting descriptor as CSV values
							this.printNodeDataFromTarget(index, descriptor);
							break;
					case 3: descriptor = args[4]; // expecting descriptor as CSV values
							int dist = Integer.parseInt(args[5]);
							this.printNodeLabelFromTargetDistance(index, descriptor, dist);
					case 4:
					case 5:
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

