
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import global.Convert;
import global.Descriptor;
import global.NID;
import global.SystemDefs;
import diskmgr.GraphDB;
import nodeheap.Node;
import nodeheap.NodeHeapfile;

class DummyNode extends Node {

	// content of the node
	public String label;
	public Descriptor desc;

	// length under control
	private int reclen;

	private byte[] data;

	/**
	 * Default constructor
	 */
	public DummyNode() {
	}

	/**
	 * another constructor
	 */
	public DummyNode(int _reclen) {
		setRecLen(_reclen);
		data = new byte[_reclen];
	}

	/**
	 * convert this class objcet to a byte array this is used when you want to
	 * write this object to a byte array
	 */
	public byte[] toByteArray() throws java.io.IOException {
		Convert.setStrValue(label, 0, data);
		Convert.setDescValue(desc, 44, data);
		return data;
	}


	// Other access methods to the size of the String field and
	// the size of the record
	public void setRecLen(int size) {
		reclen = size;
	}

	public int getRecLength() {
		return reclen;
	}
}

public class BatchNodeInsert {

	private final static int reclen = 64;
	private final static boolean OK = true;
	private final static boolean FAIL = false;

	public static void main(String []args) throws NumberFormatException, IOException {
		if (args.length > 0) {
			boolean status = OK;
			String nodeFilePath = args[0];
			String graphDB = args[1];
			GraphDB db = SystemDefs.JavabaseDB;
			NodeHeapfile nhf = db.nodeHeapfile;
			String [] vals = new String[reclen];
			Descriptor temp_desc = new Descriptor();
			for (String line : Files.readAllLines(Paths.get(nodeFilePath),StandardCharsets.US_ASCII)) {
				line = line.trim();
				vals = line.split(" ");
				temp_desc.set(Integer.parseInt(vals[1]),Integer.parseInt(vals[2]),Integer.parseInt(vals[3]),Integer.parseInt(vals[4]),Integer.parseInt(vals[5]));
				DummyNode node = new DummyNode(reclen);
				node.label = vals[0];
				node.desc = temp_desc;

				try {
					NID nid = nhf.insertNode(node.toByteArray());
					System.out.println("Node count: " + db.getNodeCnt() + "\nEdge count:" + db.getEdgeCnt());
					// TODO: No. of pages read/written
				} catch (Exception e) {
					status = FAIL;
					System.err.println("*** Error inserting node " + vals[0] + "\n");
					e.printStackTrace();
				}

			}
		} else {
			System.out.println("No inputs given\n");
		}
	}
}