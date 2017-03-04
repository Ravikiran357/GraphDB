package global;

public class EID extends RID{
	/**
	 * default constructor of class
	 */
	public EID() {
	}

	/**
	 * constructor of class
	 */
	public EID(PageId pageno, int slotno) {
		super(pageno, slotno);
	}

	/**
	 * make a copy of the given rid
	 */
	public void copyEid(EID eid) {
		copyRid(eid);
	}
}
