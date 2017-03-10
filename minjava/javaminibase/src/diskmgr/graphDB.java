package diskmgr;

import java.io.IOException;

import edgeheap.EdgeHeapfile;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.InvalidTupleSizeException;
import nodeheap.NodeHeapfile;


public class graphDB extends DB implements GlobalConst{
	private static String NODEFILENAME = "nodeheapfile";
	private static String EDGEFILENAME = "edgeheapfile";
	
	public graphDB(int type){
		//TODO
		super();
	}
	
	
	public int getNodeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException, HFException{
		int iNodeCnt = 0;
		
		NodeHeapfile nodeheapfile = new NodeHeapfile(NODEFILENAME); 
		iNodeCnt = nodeheapfile.getNodeCnt();
		return iNodeCnt;
	}
	
	public int getEdgeCnt() throws edgeheap.HFException, edgeheap.HFBufMgrException, edgeheap.HFDiskMgrException, IOException, edgeheap.InvalidSlotNumberException, edgeheap.InvalidTupleSizeException{
		int iEdgeCnt = 0;
		
		EdgeHeapfile edgeheapfile = new EdgeHeapfile(EDGEFILENAME);
		iEdgeCnt = edgeheapfile.getEdgeCnt();
		return iEdgeCnt;
		
	}
	
	public int getSourceCnt() throws edgeheap.HFException, edgeheap.HFBufMgrException, edgeheap.HFDiskMgrException, IOException, edgeheap.InvalidSlotNumberException, FieldNumberOutOfBoundException, edgeheap.InvalidTupleSizeException {
		int iSourceCnt = 0;
		
		EdgeHeapfile edgeheapfile = new EdgeHeapfile(EDGEFILENAME);
		iSourceCnt = edgeheapfile.getSourceCnt();
		return iSourceCnt;
	}
	

	public int getDestinationCnt() throws edgeheap.HFException, edgeheap.HFBufMgrException, edgeheap.HFDiskMgrException, IOException, edgeheap.InvalidSlotNumberException, FieldNumberOutOfBoundException, edgeheap.InvalidTupleSizeException{
		int iDestCnt = 0;
		
		EdgeHeapfile edgeheapfile = new EdgeHeapfile(EDGEFILENAME);
		iDestCnt = edgeheapfile.getDestinationCnt();
		return iDestCnt;
	}
	
	
	public int getLabelCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, FieldNumberOutOfBoundException, edgeheap.HFBufMgrException, edgeheap.InvalidSlotNumberException, edgeheap.InvalidTupleSizeException, IOException, edgeheap.HFException, edgeheap.HFDiskMgrException, HFException{
		int iLabelCnt =0;
		
		EdgeHeapfile edgeheapfile = new EdgeHeapfile(EDGEFILENAME);
		NodeHeapfile nodeheapfile = new NodeHeapfile(NODEFILENAME); 
		iLabelCnt = nodeheapfile.getLabelCnt() + edgeheapfile.getLabelCnt();
		
		return iLabelCnt;
	}
	
}

