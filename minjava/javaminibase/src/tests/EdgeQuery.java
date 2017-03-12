
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.NScan;
import nodeheap.Node;

import btree.BT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;

import global.Descriptor;
import global.EID;
import global.NID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import diskmgr.GraphDB;
import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.InvalidTupleSizeException;


public class EdgeQuery {

	private final static int reclen = 64;
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	private GraphDB db;
	
	private void printEdgesInHeap() throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {	
		//TODO: need to use nodeheapfile not edgeheapfile
		EScan eScan = new EScan(db.edgeHeapfile);
        EID eid = new EID();
        System.out.println("Printing edge data using edge heap file");
        Edge edge = eScan.getNext(eid);
        while(edge != null) {
        	// Print edge details
        	edge.print();
        	edge = eScan.getNext(eid);
        }
	}
	
	private void printEdgeSourceLabels(int index) throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
		HFException, HFDiskMgrException, HFBufMgrException, Exception {	
		if (index == 1) {
			//TODO: with index
			System.out.println("Printing source node labels in alphabetical order using index file");
			BTreeFile edgeIndexFile = db.edgeLabelIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				System.out.println(entry.data);
			}
		} else {
			System.out.println("Printing edge-source labels in alphabetical order using edge and node heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        boolean done = true;
	        Set<String> sourceNodeLabels = new TreeSet<String>();
	        while(done){
	            Edge edge = eScan.getNext(eid);
	            if(edge == null){
	                done = false;
	            } else {
	            	// Print Source node label
	            	NID nid = edge.getSource();
	    	        Node node = db.nodeHeapfile.getNode(nid);
	    	        if (node == null) {
	    	        	sourceNodeLabels.add("DEBUG: NODE NOT FOUND");
	    	        } else {
	    	        	sourceNodeLabels.add(node.getLabel());
	    	        }
	            }
	        }
	        for (String label : sourceNodeLabels) {
				System.out.println(label);
			}
		}
	}

	private void printEdgeDestLabels(int index) throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
	HFException, HFDiskMgrException, HFBufMgrException, Exception {	
		if (index == 1) {
			//TODO: with index
			System.out.println("Printing destination node labels in alphabetical order using index file");
			BTreeFile indexFile = db.edgeLabelIndexFile;
			BTFileScan scan = indexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				System.out.println(entry.data);
			}
		} else {
			System.out.println("Printing edge-dest labels in alphabetical order using edge and node heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        boolean done = true;
	        Set<String> destNodeLabels = new TreeSet<String>();
	        while(done){
	            Edge edge = eScan.getNext(eid);
	            if(edge == null){
	                done = false;
	            } else {
	            	// Print Destination node label
	            	NID nid = edge.getDestination();
	    	        Node node = db.nodeHeapfile.getNode(nid);
	    	        if (node == null) {
	    	        	destNodeLabels.add("DEBUG: NODE NOT FOUND");
	    	        } else {
	    	        	destNodeLabels.add(node.getLabel());
	    	        }
	            }
	        }
	        for (String label : destNodeLabels) {
				System.out.println(label);
			}
		}
	}	
	
	private void printEdgeLabels(int index) throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
	HFException, HFDiskMgrException, HFBufMgrException, Exception {	
		if (index == 1) {
			System.out.println("Printing edge labels in alphabetical order using index file");
			BTreeFile indexFile = db.edgeLabelIndexFile;
			BTFileScan scan = indexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				System.out.println(entry.data);
			}
		} else {
			System.out.println("Printing edge labels in alphabetical order using edge heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        List<String> edgeLabels = new ArrayList<String>();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
            	// Print edge label
            	edgeLabels.add(edge.getLabel());
            	edge = eScan.getNext(eid);
	        }
	        Collections.sort(edgeLabels);
	        for (String label : edgeLabels) {
				System.out.println(label);
			}
		}
	}
	
	private void printEdgeWeights(int index, KeyClass low, KeyClass high) throws InvalidSlotNumberException, 
		nodeheap.InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		if (index == 1) {
			System.out.println("Printing edge weights in order using index file");
			BTreeFile indexFile = db.edgeWeightIndexFile;
			BTFileScan scan = indexFile.new_scan(low,high);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				System.out.println(entry.data);
			}
		} else {
			System.out.println("Printing edge weights in order using edge heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        List<Integer> edgeWeights = new ArrayList<Integer>();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
            	// Print edge weight
	        	edgeWeights.add(edge.getWeight());
            	edge = eScan.getNext(eid);
	        }
	        Collections.sort(edgeWeights);
	        for (int label : edgeWeights) {
				System.out.println(label);
			}
		}
	}
	
	//TODO: NOT DONE
	private void printIncidentEdges(int index) throws InvalidSlotNumberException, 
	nodeheap.InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		if (index == 1) {
			System.out.println("Printing edge weights in order using index file");
			BTreeFile indexFile = db.edgeWeightIndexFile;
			BTFileScan scan = indexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				System.out.println(entry.data);
			}
		} else {
			System.out.println("Printing edge weights in order using edge heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        List<Integer> edgeWeights = new ArrayList<Integer>();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
	        	// Print edge weight
	        	edgeWeights.add(edge.getWeight());
	        	edge = eScan.getNext(eid);
	        }
	        Collections.sort(edgeWeights);
	        for (int label : edgeWeights) {
				System.out.println(label);
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
				String queryOptions = args[4];
				this.db = SystemDefs.JavabaseDB;
				String [] vals = new String[reclen];
				
				switch(qType) {
					case 0: this.printEdgesInHeap();
							break;
					case 1: this.printEdgeSourceLabels(index);
							break;
					case 2: this.printEdgeDestLabels(index);
							break;
					case 3: this.printEdgeLabels(index);
							break;
					case 4: this.printEdgeWeights(index, null, null);
							break;
					case 5: KeyClass lowkey, hikey;
							lowkey = new IntegerKey(Integer.parseInt(args[4]));
							hikey = new IntegerKey(Integer.parseInt(args[5]));
							this.printEdgeWeights(index, lowkey, hikey);
							break;
					case 6: this.printIncidentEdges(index);
					default:
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Failed edge query");
			}
		} else {
			status = FAIL;
			System.out.println("No inputs given\n");
		}
		return status;
	}
}

