package iterator;

import diskmgr.GraphDB;
import edgeheap.Edge;
import edgeheap.EdgeHeapfile;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.NID;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

import java.io.IOException;

import nodeheap.Node;
import nodeheap.NodeHeapfile;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import bufmgr.PageNotReadException;

public class NestedLoopExtended extends Iterator {
	private GraphDB db = SystemDefs.JavabaseDB;
	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator outer;
	private short t2_str_sizescopy[];
	private CondExpr OutputFilter[];
	private CondExpr RightFilter[];
	private int n_buf_pgs; // # of buffer pages available.
	private boolean done, // Is the join complete
			get_from_outer; // if TRUE, a tuple is got from outer
	private Tuple outer_tuple;
	private Edge edge;

	KeyDataEntry inner_tuple;
	private Tuple Jtuple; // Joined tuple
	private FldSpec perm_mat[];
	private int nOutFlds;
	private NodeHeapfile nhf;
	BTreeFile esif;
	private BTFileScan inner;

	/**
	 * constructor Initialize the two relations which are joined, including
	 * relation type,
	 * 
	 * @param in1
	 *            Array containing field types of R.
	 * @param len_in1
	 *            # of columns in R.
	 * @param t1_str_sizes
	 *            shows the length of the string fields.
	 * @param in2
	 *            Array containing field types of S
	 * @param len_in2
	 *            # of columns in S
	 * @param t2_str_sizes
	 *            shows the length of the string fields.
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left i/p to join
	 * @param relationName
	 *            access hfapfile for right i/p to join
	 * @param outFilter
	 *            select expressions
	 * @param rightFilter
	 *            reference to filter applied on right i/p
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fileds
	 * @exception IOException
	 *                some I/O fault
	 * @exception NestedLoopException
	 *                exception from this class
	 */
	public NestedLoopExtended(AttrType in1[], int len_in1, short t1_str_sizes[], AttrType in2[], int len_in2,
			short t2_str_sizes[], int amt_of_mem, Iterator am1, String relationName, CondExpr outFilter[],
			CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds) throws IOException, NestedLoopException {

		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in1_len = len_in1;
		in2_len = len_in2;

		outer = am1;
		t2_str_sizescopy = t2_str_sizes;
		//inner_tuple = new KeyDataEntry(null, null);
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter = rightFilter;
		n_buf_pgs = amt_of_mem;
		inner = null;
		done = false;
		
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in2, len_in2, t1_str_sizes, t2_str_sizes,
					proj_list, nOutFlds);
		} catch (TupleUtilsException e) {
			throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
		}

		try {
			//pass relationName as "nodeheapfile" or "edgeheapfile"
			if(relationName.equals("edgeSourceIndexFile"))
			{esif = db.edgeSourceIndexFile;}
			if(relationName.equals("edgeDestinationIndexFile"))
			{esif = db.edgeDestinationIndexFile;}
			//inner = esif.new_scan(hi,hi);
			

		} catch (Exception e) {
			throw new NestedLoopException(e, "Open index failed.");
		}
	}

	/**
	 * @return The joined tuple is returned
	 * @exception IOException
	 *                I/O errors
	 * @exception JoinsException
	 *                some join exception
	 * @exception IndexException
	 *                exception from super class
	 * @exception InvalidTupleSizeException
	 *                invalid tuple size
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception PageNotReadException
	 *                exception from lower layer
	 * @exception TupleUtilsException
	 *                exception from using tuple utilities
	 * @exception PredEvalException
	 *                exception from PredEval class
	 * @exception SortException
	 *                sort exception
	 * @exception LowMemException
	 *                memory error
	 * @exception UnknowAttrType
	 *                attribute type unknown
	 * @exception UnknownKeyTypeException
	 *                key type unknown
	 * @exception Exception
	 *                other exceptions
	 * 
	 */
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// This is a DUMBEST form of a join, not making use of any key
		// information...

		if (done)
			return null;

		do {
			// If get_from_outer is true, Get a tuple from the outer, delete
			// an existing scan on the file, and reopen a new scan on the file.
			// If a get_next on the outer returns DONE?, then the nested loops
			// join is done too.
			//Jtuple = new Tuple();
			if (get_from_outer == true) {
				get_from_outer = false;
				if (inner != null) // If this not the first time,
				{
					// close scan
					//esif.close();
					inner.DestroyBTreeFileScan();
					inner = null;
				}
				

				

				if ((outer_tuple = outer.get_next()) == null) {
					done = true;
					if (inner != null) {
						inner.DestroyBTreeFileScan();
						inner = null;
					}

					return null;
				}
				try {
					StringKey low = new StringKey(outer_tuple.getStrFld(1));
					StringKey hi = new StringKey("5");
					
					inner = esif.new_scan(low,low);
					
				} catch (Exception e) {
					throw new NestedLoopException(e, "openScan failed");
				}
			} // ENDS: if (get_from_outer == TRUE)
			
			// The next step is to get a tuple from the inner,
			// while the inner is not completely scanned && there
			// is no match (with pred),get a tuple from the inner.

			EID eid = new EID();
			while ((inner_tuple = inner.get_next()) != null) {
				LeafData leafData = (LeafData) inner_tuple.data;
				eid.copyRid(leafData.getData());
				edge = db.edgeHeapfile.getEdge(eid);
				//System.out.println(outer_tuple.getStrFld(1));
				Descriptor desc = outer_tuple.getDescFld(2);
//				for(int j= 0; j<5;j++){
//		            System.out.print(desc.get(j)+"  ");
//		        }
				//edge.print();
				//nodeList.add(node);
				String labelofNode = ((StringKey)inner_tuple.key).getKey();
				//entry = scan.get_next();
				//inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
				//System.out.println("SourceLabel"+labelofSource);
				//System.out.println(outer_tuple.getStrFld(1));
				if (PredEval.Eval(RightFilter, null, edge, null, _in2) == true) {
					if (PredEval.Eval(OutputFilter, outer_tuple, null, _in1, null) == true) {
					//if (outer_tuple.getStrFld(1).equals(labelofNode)) {
						// Apply a projection on the outer and inner tuples.
						Projection.Join(outer_tuple, _in1, edge, _in2, Jtuple, perm_mat, nOutFlds);
						//get_from_outer = true;
						return Jtuple;
					}
				}
			}
			

			// There has been no match. (otherwise, we would have
			// returned from t//he while loop. Hence, inner is
			// exhausted, => set get_from_outer = TRUE, go to top of loop
			get_from_outer = true;
			 // Loop back to top and get next outer tuple.
		} while (true);
	}

	/**
	 * implement the abstract method close() fr7om super class Iterator to finish
	 * cleaning up
	 * 
	 * @exception IOException
	 *                I/O error from lower layers
	 * @exception JoinsException
	 *                join error from lower layers
	 * @exception IndexException
	 *                index access error
	 */
	public void close() throws JoinsException, IOException, IndexException {
		if (!closeFlag) {

			try {
				outer.close();
				if(inner != null) inner.DestroyBTreeFileScan();
			} catch (Exception e) {
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}