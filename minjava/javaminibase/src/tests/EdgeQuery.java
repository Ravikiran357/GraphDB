package tests;
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
import btree.LeafData;
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
	
	private void printEdgesInHeap() throws InvalidTupleSizeException, IOException, 
		FieldNumberOutOfBoundException {	
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
        eScan.closescan();
	}
	
	private void printEdgeSourceLabels(int index) throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
		HFException, HFDiskMgrException, HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
		if (index == 1) {
			// TODO: Not using the right index for this query
			System.out.println("Printing edge data in alphanumerical order of source node labels using index file");
			BTreeFile edgeIndexFile = db.edgeLabelIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect edge data
				LeafData leafData = (LeafData) entry.data;
				Edge edge = db.edgeHeapfile.getEdge((EID) leafData.getData());
				edgeList.add(edge);
			}
			scan.DestroyBTreeFileScan();
		} else {
			System.out.println("Printing edge data in alphanumerical order of source node labels using edge and node heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
            	// Collect edge data
	        	edgeList.add(edge);
	        	edge = eScan.getNext(eid);
	        }
	        eScan.closescan();
	        Collections.sort(edgeList, new Comparator<Edge>() {
	            public int compare(Edge e1,Edge e2) {
	            	try {
	            		NID nid1 = new NID();
	            		nid1 = e1.getSource();
	            		Node node1 = db.nodeHeapfile.getNode(nid1);
	            		NID nid2 = new NID();
	            		nid2 = e2.getSource();
	            		Node node2 = db.nodeHeapfile.getNode(nid2);
						return node1.getLabel().compareTo(node2.getLabel());
					} catch (FieldNumberOutOfBoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InvalidSlotNumberException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (nodeheap.InvalidTupleSizeException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (HFException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (HFDiskMgrException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (HFBufMgrException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return 0;
	            }
	        });
		}
	    for (Edge edge : edgeList) {
			edge.print();
		}
	}

	private void printEdgeDestLabels(int index) throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
	HFException, HFDiskMgrException, HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
		if (index == 1) {
			// TODO: Not using the right index for this query
			System.out.println("Printing edge data in alphanumerical order of destination node labels using index file");
			BTreeFile edgeIndexFile = db.edgeLabelIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect edge data
				LeafData leafData = (LeafData) entry.data;
				Edge edge = db.edgeHeapfile.getEdge((EID) leafData.getData());
				edgeList.add(edge);
			}
			scan.DestroyBTreeFileScan();
		} else {
			System.out.println("Printing edge data in alphanumerical order of destination node labels using edge and node heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
	        	// Collect edge data
	        	edgeList.add(edge);
	        	edge = eScan.getNext(eid);
	        }
	        eScan.closescan();
	        // Sorting the data by destination labels
	        Collections.sort(edgeList, new Comparator<Edge>() {
	            public int compare(Edge e1,Edge e2) {
	            	try {
	            		NID nid1 = new NID();
	            		nid1 = e1.getSource();
	            		Node node1 = db.nodeHeapfile.getNode(nid1);
	            		NID nid2 = new NID();
	            		nid2 = e2.getSource();
	            		Node node2 = db.nodeHeapfile.getNode(nid2);
						return node1.getLabel().compareTo(node2.getLabel());
					} catch (FieldNumberOutOfBoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InvalidSlotNumberException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (nodeheap.InvalidTupleSizeException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (HFException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (HFDiskMgrException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (HFBufMgrException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return 0;
	            }
	        });
		}
	    for (Edge edge : edgeList) {
			edge.print();
		}
	}

	private void printEdgeLabels(int index) throws InvalidSlotNumberException, 
		nodeheap.InvalidTupleSizeException, HFException, HFDiskMgrException, 
		HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
		if (index == 1) {
			System.out.println("Printing edge data in alphanumerical order of edge labels using index file");
			BTreeFile edgeIndexFile = db.edgeLabelIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect edge data
				LeafData leafData = (LeafData) entry.data;
				Edge edge = db.edgeHeapfile.getEdge((EID) leafData.getData());
				edgeList.add(edge);
			}
			scan.DestroyBTreeFileScan();
		} else {
			System.out.println("Printing edge data in alphanumerical order of edge labels using edge and node heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
	        	// Collect edge data
	        	edgeList.add(edge);
	        	edge = eScan.getNext(eid);
	        }
	        eScan.closescan();
	        // Sorting the data by edge labels
	        Collections.sort(edgeList, new Comparator<Edge>() {
	            public int compare(Edge e1,Edge e2) {
	            	try {
						return e1.getLabel().compareTo(e2.getLabel());
					} catch (FieldNumberOutOfBoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return 0;
	            }
	        });
		}
	    for (Edge edge : edgeList) {
			edge.print();
		}
	}
	
	private void printEdgeWeights(int index, KeyClass low, KeyClass high) throws 
		InvalidSlotNumberException,	nodeheap.InvalidTupleSizeException, HFException, 
		HFDiskMgrException, HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
		if (index == 1) {
			System.out.println("Printing edge data in order of weights using index file");
			BTreeFile edgeIndexFile = db.edgeWeightIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(low,high);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect edge data
				LeafData leafData = (LeafData) entry.data;
				Edge edge = db.edgeHeapfile.getEdge((EID) leafData.getData());
				edgeList.add(edge);
			}
			scan.DestroyBTreeFileScan();
		} else {
			System.out.println("Printing edge data in order of weights using edge and node heap file");
			EScan eScan = new EScan(db.edgeHeapfile);
	        EID eid = new EID();
	        Edge edge = eScan.getNext(eid);
	        while(edge != null){
	        	// Collect edge data
	        	edgeList.add(edge);
	        	edge = eScan.getNext(eid);
	        }
	        eScan.closescan();
	        // Sorting the data by edge weights
	        Collections.sort(edgeList, new Comparator<Edge>() {
	            public int compare(Edge e1,Edge e2) {
	            	try {
						return e1.getWeight() - e2.getWeight();
					} catch (FieldNumberOutOfBoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return 0;
	            }
	        });
		}
	    for (Edge edge : edgeList) {
			edge.print();
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

	public boolean evaluate(int qType, int index, String []args) {
		boolean status = OK;
		if (args.length > 0) {
			try {
				this.db = SystemDefs.JavabaseDB;
				
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
							break;
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

