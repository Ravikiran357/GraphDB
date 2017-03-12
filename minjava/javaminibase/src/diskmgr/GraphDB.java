package diskmgr;

import java.io.IOException;

import zIndex.ZTreeFile;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;

import edgeheap.EdgeHeapfile;
import global.AttrType;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.InvalidTupleSizeException;
import nodeheap.NodeHeapfile;


public class GraphDB extends DB implements GlobalConst{
	private static String NODEFILENAME = "nodeheapfile";
	private static String EDGEFILENAME = "edgeheapfile";
	public NodeHeapfile nodeHeapfile;
	public EdgeHeapfile edgeHeapfile;
	public BTreeFile nodeLabelIndexFile;
	public ZTreeFile nodeDescriptorIndexFile;
	public BTreeFile edgeLabelIndexFile;
	public BTreeFile edgeWeightIndexFile;

	public GraphDB(int type) {
		super();
	}

	public void createFiles() throws HFException, HFBufMgrException, HFDiskMgrException, IOException, 
		GetFileEntryException, ConstructPageException, AddFileEntryException, edgeheap.HFException, 
		edgeheap.HFBufMgrException, edgeheap.HFDiskMgrException, PinPageException {
		this.nodeHeapfile = new NodeHeapfile(NODEFILENAME);
		this.edgeHeapfile = new EdgeHeapfile(EDGEFILENAME);
		this.nodeLabelIndexFile = new BTreeFile("NodeLabel", AttrType.attrString, 44, 1);
		this.nodeDescriptorIndexFile = new ZTreeFile("NodeDescriptor");
		this.edgeLabelIndexFile = new BTreeFile("EdgeLabel", AttrType.attrString, 44, 1);
		this.edgeWeightIndexFile = new BTreeFile("EdgeWeight", AttrType.attrInteger, 4, 1);
	}
	
	public int getNodeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, 
		HFBufMgrException, IOException, HFException{
		int iNodeCnt = 0;
		iNodeCnt = nodeHeapfile.getNodeCnt();
		return iNodeCnt;
	}
	
	public int getEdgeCnt() throws edgeheap.HFException, edgeheap.HFBufMgrException, edgeheap.HFDiskMgrException,
		IOException, edgeheap.InvalidSlotNumberException, edgeheap.InvalidTupleSizeException{
		int iEdgeCnt = 0;
		iEdgeCnt = edgeHeapfile.getEdgeCnt();
		return iEdgeCnt;
		
	}
	
	public int getSourceCnt() throws edgeheap.HFException, edgeheap.HFBufMgrException, 
		edgeheap.HFDiskMgrException, IOException, edgeheap.InvalidSlotNumberException, 
		FieldNumberOutOfBoundException, edgeheap.InvalidTupleSizeException {
		int iSourceCnt = 0;
		iSourceCnt = edgeHeapfile.getSourceCnt();
		return iSourceCnt;
	}
	

	public int getDestinationCnt() throws edgeheap.HFException, edgeheap.HFBufMgrException, edgeheap.HFDiskMgrException, 
		IOException, edgeheap.InvalidSlotNumberException, FieldNumberOutOfBoundException, edgeheap.InvalidTupleSizeException{
		int iDestCnt = 0;
		iDestCnt = edgeHeapfile.getDestinationCnt();
		return iDestCnt;
	}
	
	
	public int getLabelCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, 
		HFBufMgrException, FieldNumberOutOfBoundException, edgeheap.HFBufMgrException, edgeheap.InvalidSlotNumberException, 
		edgeheap.InvalidTupleSizeException, IOException, edgeheap.HFException, edgeheap.HFDiskMgrException, HFException{
		int iLabelCnt =0;
		iLabelCnt = nodeHeapfile.getLabelCnt() + edgeHeapfile.getLabelCnt();
		return iLabelCnt;
	}
}

