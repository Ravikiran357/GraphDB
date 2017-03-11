package tests;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.InvalidTupleSizeException;
import global.EID;
import global.NID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import nodeheap.NScan;
import nodeheap.Node;

import java.io.IOException;

/**
 * Created by revu on 3/10/17.
 */
public class BatchEdgeInsert {

    BatchEdgeInsert(){

    }
    public NID getNode(String nodeLabel) throws InvalidTupleSizeException, IOException, edgeheap.FieldNumberOutOfBoundException, nodeheap.InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
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

    public EID getEdge(String edgeLabel) throws edgeheap.InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {
        EScan escan = new EScan(SystemDefs.JavabaseDB.edgeHeapfile);
        EID eid = new EID();
        boolean done = true;
        while(done){
            Edge e  = escan.getNext(eid);
            if(e == null){
                done = false;
            }
            if(e.getLabel().equals(edgeLabel)){
                return eid;
            }

        }
        return null;
    }


    public void doSingleBatchEdgeDelete  (String sourceLabel, String destLabel, String edgeLabel, Integer edgeWeight) throws Exception {


        EID eid = getEdge(edgeLabel);
        Edge edge = new Edge();

        NID sourceNID = getNode(sourceLabel);
        NID destNID = getNode(destLabel);

        edge.setLabel(edgeLabel);
        edge.setSource(sourceNID);
        edge.setDestination(destNID);
        edge.setWeight(edgeWeight);
        SystemDefs.JavabaseDB.edgeHeapfile.insertEdge(edge.getEdgeByteArray());

    }

}
