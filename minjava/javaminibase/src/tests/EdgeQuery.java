package tests;


import java.io.IOException;
import java.util.*;

import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.Node;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.LeafData;

import global.EID;
import global.NID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import diskmgr.GraphDB;
import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.InvalidTupleSizeException;


public class EdgeQuery {

	private final static boolean OK = true;
	private final static boolean FAIL = false;
	private GraphDB db;
	
	private void printEdgesInHeap() throws InvalidTupleSizeException, IOException, 
		FieldNumberOutOfBoundException {
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
	
	private void printEdgeSourceLabels() throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
		HFException, HFDiskMgrException, HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
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
        // Sorting the data by source labels
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
					e.printStackTrace();
				} catch (nodeheap.InvalidTupleSizeException e) {
					e.printStackTrace();
				} catch (HFException e) {
					e.printStackTrace();
				} catch (HFDiskMgrException e) {
					e.printStackTrace();
				} catch (HFBufMgrException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
            }
        });
	    for (Edge e : edgeList) {
			e.print();
		}
	}

	private void printEdgeDestLabels() throws InvalidSlotNumberException, nodeheap.InvalidTupleSizeException,
	HFException, HFDiskMgrException, HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
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
            		nid1 = e1.getDestination();
            		Node node1 = db.nodeHeapfile.getNode(nid1);
            		NID nid2 = new NID();
            		nid2 = e2.getDestination();
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
	    for (Edge e : edgeList) {
			e.print();
		}
	}

	private void printEdgeLabels(int index) throws InvalidSlotNumberException, 
		nodeheap.InvalidTupleSizeException, HFException, HFDiskMgrException, 
		HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
		if (index == 1) {
			System.out.println("Printing edge data in alphanumerical order of edge labels using index file");
			EID eid = new EID();
			BTreeFile edgeIndexFile = db.edgeLabelIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect edge data
				LeafData leafData = (LeafData) entry.data;
				eid.copyRid(leafData.getData());
				Edge edge = db.edgeHeapfile.getEdge(eid);
				edgeList.add(edge);
				entry = scan.get_next();
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
		nodeheap.InvalidTupleSizeException, HFException, HFDiskMgrException, 
		HFBufMgrException, Exception {
		List<Edge> edgeList = new ArrayList<Edge>();
		if (index == 1) {
			System.out.println("Printing edge data in order of weights using index file");
			EID eid = new EID();
			BTreeFile edgeIndexFile = db.edgeWeightIndexFile;
			BTFileScan scan = edgeIndexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				// Collect edge data
				LeafData leafData = (LeafData) entry.data;
				eid.copyRid(leafData.getData());
				Edge edge = db.edgeHeapfile.getEdge(eid);
				edgeList.add(edge);
				entry = scan.get_next();
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
						return (e1.getWeight() - e2.getWeight());
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

	private void printIncidentEdges(int index) throws 
		nodeheap.InvalidTupleSizeException, HFException, HFDiskMgrException, 
		HFBufMgrException, Exception {
		List<HashMap<Edge,Edge>> edgeList = new ArrayList<HashMap<Edge,Edge>>();
		HashMap<Edge,Edge> hash;
		System.out.println("Printing data of incident edges using edge heap file");
		EScan eScan1 = new EScan(db.edgeHeapfile);
		EScan eScan2 = new EScan(db.edgeHeapfile);
        EID eid1 = new EID();
        EID eid2 = new EID();
        Edge edge1 = eScan1.getNext(eid1);
        Edge edge2 = eScan2.getNext(eid2);
        while(edge1 != null){
        	while(edge2 != null){
	        	// Check if its duplicate edges using the labels and
        		// if the source node of first edge is same as destination node of second edge
	        	if ((edge1.getLabel() != edge2.getLabel()) && (edge1.getSource() == edge2.getDestination())) {
	        		hash = new HashMap<Edge,Edge>();
	        		hash.put(edge1, edge2);
	        		edgeList.add(hash);
	        	}
	        	edge1 = eScan1.getNext(eid1);
	        	edge2 = eScan2.getNext(eid2);
        	}
        }
        eScan1.closescan();
        eScan2.closescan();
	    for (Map<Edge,Edge> map : edgeList) {
	    	for (Map.Entry<Edge,Edge> entry : map.entrySet()) {
				Edge key = entry.getKey();
				key.print();
				Edge value = entry.getValue();
				value.print();
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
				
				switch(qType) {
					case 0: this.printEdgesInHeap();
							break;
					case 1: this.printEdgeSourceLabels();
							break;
					case 2: this.printEdgeDestLabels();
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

