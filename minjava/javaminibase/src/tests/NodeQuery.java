
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import btree.BT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;

import global.Descriptor;
import global.NID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import diskmgr.GraphDB;
import nodeheap.InvalidTupleSizeException;
import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;


public class NodeQuery {

	private final static boolean OK = true;
	private final static boolean FAIL = false;
	private GraphDB db;
	
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
	}
	
	private void printNodeLabels(int index) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException, 
		KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, ScanIteratorException {	
		if (index == 1) {
			System.out.println("Printing node labels in alphabetical order using index file");
			BTreeFile indexFile = db.nodeLabelIndexFile;
			BTFileScan scan = indexFile.new_scan(null,null);
			KeyDataEntry entry = scan.get_next();
			while (entry != null) {
				System.out.println(entry.data);
			}
		} else {
			System.out.println("Printing node labels in alphabetical order using node heap file");
			NScan nScan = new NScan(db.nodeHeapfile);
	        NID nid = new NID();
	        List<String> nodeLabels = new ArrayList<String>();
	        Node node = nScan.getNext(nid);
	        while(node != null){
            	// Print Node label
            	nodeLabels.add(node.getLabel());
	        }
	        Collections.sort(nodeLabels);
	        for (String label : nodeLabels) {
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
	
				switch(qType) {
					case 0: this.printNodesInHeap();
							break;
					case 1: this.printNodeLabels(index);
							break;
					case 2:
					case 3:
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

