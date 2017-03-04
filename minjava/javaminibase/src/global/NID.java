package global;

/*  File NID.java   */
/**
 * class NID
 */

public class NID extends RID{

	/**
	 * default constructor of class
	 */
	public NID() {
	}

	/**
	 * constructor of class
	 */
	public NID(PageId pageno, int slotno) {
		super(pageno, slotno);
	}

	/**
	 * make a copy of the given rid
	 */
	public void copyNid(NID nid) {
		copyRid(nid);
	}

}

