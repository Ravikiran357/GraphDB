package tests;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IntegerKey;
import btree.PinPageException;
import btree.StringKey;
import edgeheap.*;
import edgeheap.FieldNumberOutOfBoundException;
import edgeheap.InvalidTupleSizeException;
import global.*;
import nodeheap.*;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import zIndex.DescriptorKey;
import zIndex.ZTreeFile;

public class BatchNodeDelete implements GlobalConst{
    private String nodeLabel;
    
	
    public BatchNodeDelete() {
    	
    }
    
    public NID getNode(String nodeLabel) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException, nodeheap.InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
    	NScan nScan = new NScan(SystemDefs.JavabaseDB.nodeHeapfile);
    	NID nid = new NID();
        boolean done = true;
        while(done){
            Node n  = nScan.getNext(nid);
            if(n == null){
                done = false;
                
                break;
            }
            if(n.getLabel().equals(nodeLabel)){
            	nScan.closescan();
                return nid;
            }
        }
        return null;
    }
    
    public void doSingleBatchNodeDelete(String line) throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception{
        line = line.trim();
        String [] vals = new String[5];

        vals = line.split(" ");
        this.nodeLabel = vals[0];
    	NID nid = getNode(nodeLabel);
    	Node node = new Node();
    	
    	EScan eScan = new EScan(SystemDefs.JavabaseDB.edgeHeapfile);
    	EID eid = new EID();
        boolean done = true;
        while(done){
            Edge e  = eScan.getNext(eid);
            if(e == null){
                done = false;
                eScan.closescan();
                break;
            }
            
            if(e.getSource().equals(nid) || e.getDestination().equals(nid)){
            	SystemDefs.JavabaseDB.edgeHeapfile.deleteEdge(eid);
            	SystemDefs.JavabaseDB.edgeLabelIndexFile.Delete(new StringKey(e.getLabel()), eid);
            	SystemDefs.JavabaseDB.edgeWeightIndexFile.Delete(new IntegerKey(e.getWeight()), eid);
            }
        }
        SystemDefs.JavabaseDB.nodeHeapfile.deleteNode(nid);
        SystemDefs.JavabaseDB.nodeLabelIndexFile.Delete(new StringKey(node.getLabel()), nid);
        SystemDefs.JavabaseDB.nodeDescriptorIndexFile.Delete(new DescriptorKey(node.getDesc()), nid);
    }
}
