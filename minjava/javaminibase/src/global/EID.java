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
	public void copyRid(RID rid) {
		pageNo = rid.pageNo;
		slotNo = rid.slotNo;
	}
}
