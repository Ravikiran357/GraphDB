
import nodeheap.Node;

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
	 * constructor: convert a byte array to DummyNode object.
	 * 
	 * @param arecord
	 *            a byte array which represents the DummyNode object
	 */
	public DummyNode(byte[] arecord) throws java.io.IOException {
		setIntRec(arecord);
		setFloRec(arecord);
		setStrRec(arecord);
		data = arecord;
		setRecLen(name.length());
	}

	/**
	 * constructor: translate a tuple to a DummyNode object it will make a
	 * copy of the data in the tuple
	 * 
	 * @param atuple:
	 *            the input tuple
	 */
	public DummyNode(Node node) throws java.io.IOException {
		data = new byte[node.getLength()];
		data = node.getTupleByteArray();
		setRecLen(node.getLength());

		setIntRec(data);
		setFloRec(data);
		setStrRec(data);

	}

	/**
	 * convert this class objcet to a byte array this is used when you want to
	 * write this object to a byte array
	 */
	public byte[] toByteArray() throws java.io.IOException {
		// data = new byte[reclen];
		Convert.setStrValue(name, 0, data);
		Convert.setDescValue(fval, 44, data);
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

	public static void main(String args) {
		if (args.length > 0) {
			String nodeFilePath = args[0];
			String graphDB = args[1];
			GraphDB db = new GraphDB(0);
			NodeHeapFile nhf = db.nodeHeapFile;
			String [] vals = new String[reclen];
			Descriptor temp_desc = new Descriptor();
			for (String line : Files.readAllLines(Paths.get(nodeFilePath))) {
				nhf.insertNode();
				line = line.trim();
				vals = line.split(" ");
				temp_desc.set(vals[1],vals[2],vals[3],vals[4],vals[5]);
				DummyNode node = new DummyNode(reclen);
				node.label = vals[0];
				node.desc = temp_desc;

				try {
					nid = nhf.insertRecord(node.toByteArray());
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