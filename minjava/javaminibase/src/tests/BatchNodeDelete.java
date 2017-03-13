package tests;

import java.io.IOException;

import edgeheap.*;
import edgeheap.FieldNumberOutOfBoundException;
import edgeheap.InvalidTupleSizeException;
import global.*;
import nodeheap.*;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;

public class BatchNodeDelete implements GlobalConst{
    private String nodeLabel;
    
    public BatchNodeDelete(){
    }
    
    public NID getNode(String nodeLabel) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException, nodeheap.InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
    	NScan nScan = new NScan(SystemDefs.JavabaseDB.nodeHeapfile);
    	NID nid = new NID();
        boolean done = true;
        while(done){
            Node n  = nScan.getNext(nid);
            if(n == null){
                done = false;
            }
            if(n.getLabel().equals(nodeLabel)){
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

    	EScan eScan = new EScan(SystemDefs.JavabaseDB.edgeHeapfile);
    	EID eid = new EID();
        boolean done = true;
        while(done){
            Edge e  = eScan.getNext(eid);
            if(e == null){
                done = false;
            }

//            if(e.getSource().equals(nid) || e.getDestination().equals(nid)){
//            	SystemDefs.JavabaseDB.edgeHeapfile.deleteEdge(eid);
//            }
        }
        SystemDefs.JavabaseDB.nodeHeapfile.deleteNode(nid);
    }
}
