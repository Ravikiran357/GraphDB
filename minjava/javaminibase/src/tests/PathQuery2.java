package tests;
// Task 4,7

import java.io.IOException;

import edgeheap.Edge;
import zIndex.DescriptorKey;
import zIndex.ZTreeFile;

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

import global.Descriptor;
import global.EID;
import global.NID;
import global.SystemDefs;

public class PathQuery2 {
	private String [] edge_path;
	private int no_of_edges;

	PathQuery2(String path, boolean is_desc) throws nodeheap.InvalidTupleSizeException,
		IOException, KeyNotMatchException, IteratorException, ConstructPageException, 
		PinPageException, UnpinPageException, InvalidFrameNumberException, ReplacerException, 
		PageUnpinnedException, HashEntryNotFoundException, ScanIteratorException {
		no_of_edges = path.length() - path.replace("/", "").length();
		edge_path = new String[no_of_edges + 1];
		edge_path = path.split("/");
		BTFileScan iscan;
		if (is_desc) {
			String[] node_desc = new String[5];
			node_desc = edge_path[0].split(",");
			ZTreeFile node_index = SystemDefs.JavabaseDB.nodeDescriptorIndexFile;
			Descriptor node_key = new Descriptor();
			node_key.set(Integer.parseInt(node_desc[0]),Integer.parseInt(node_desc[1]),
					Integer.parseInt(node_desc[2]),Integer.parseInt(node_desc[3]), 
					Integer.parseInt(node_desc[4]));
			iscan = node_index.new_scan(new DescriptorKey(node_key), new DescriptorKey(node_key));
		} else {
			BTreeFile node_index = SystemDefs.JavabaseDB.nodeLabelIndexFile;
			iscan = node_index.new_scan(new StringKey(edge_path[0]), new StringKey(edge_path[0]));
		}
		KeyDataEntry entry = iscan.get_next();
		while (entry != null) {
			// Get NID
//			LeafData leafData = (LeafData) entry.data;
//			first_nid = new NID();
//			first_nid.copyRid(leafData.getData());
			break;
		}
		iscan.DestroyBTreeFileScan();	
	}
	
	private Edge getNextindexFilterSource(BTFileScan iscan, String edgeLabel) throws edgeheap.InvalidSlotNumberException, edgeheap.InvalidTupleSizeException, edgeheap.HFException, edgeheap.HFDiskMgrException, edgeheap.HFBufMgrException, Exception{
		KeyDataEntry keyData = iscan.get_next();
		if (keyData == null)
			return null;
		LeafData edgeLeaf =  (LeafData)keyData.data;
		EID edgeId = new EID();
		edgeId.copyRid(edgeLeaf.getData());
		Edge e = SystemDefs.JavabaseDB.edgeHeapfile.getEdge(edgeId);
		if (edgeLabel == null )
			return e;

		if (edgeLabel.equals(e.getLabel())){
			return e;
		} else {
			return getNextindexFilterSource(iscan, edgeLabel);
		}
	}
	
	/*
	 * input parameters
	 * outer destination_label
	 * inner source_label
	 */
	public void NestedLoopJoin(String sourceNodeLabel, int edgeLabelIndex) throws edgeheap.InvalidSlotNumberException, 
		edgeheap.InvalidTupleSizeException, edgeheap.HFException, edgeheap.HFDiskMgrException, 
		edgeheap.HFBufMgrException, Exception {
		BTreeFile sourceNodeIndexFile = SystemDefs.JavabaseDB.edgeSourceIndexFile;
		BTFileScan iscan = sourceNodeIndexFile.new_scan(new StringKey(sourceNodeLabel), new StringKey(sourceNodeLabel));
		Edge e = getNextindexFilterSource(iscan, edge_path[edgeLabelIndex]);
//		while(e != null) {
//			e.print();
//			e = getNextindexFilterSource(iscan, edge_path[edgeLabelIndex]);
//			}
		while (e != null) {
			// if all the joins are performed print the tail
			if (edgeLabelIndex == no_of_edges) {
				System.out.println("!!!!!!!");
				e.print();				
			} else {
				e.print();
				String sourceLabel = SystemDefs.JavabaseDB.nodeHeapfile.getNode(
						e.getDestination()).getLabel();//e's destination which will be source to inner guy
				//edgeLabelIndex++;
				NestedLoopJoin(sourceLabel, edgeLabelIndex+1);
			}
			e = getNextindexFilterSource(iscan, edge_path[edgeLabelIndex]);
		}		
	}
	
	public void joinOperation() throws edgeheap.InvalidSlotNumberException, edgeheap.InvalidTupleSizeException, 
		edgeheap.HFException, edgeheap.HFDiskMgrException, edgeheap.HFBufMgrException, Exception {
		NestedLoopJoin(edge_path[0], 1);
	}
}