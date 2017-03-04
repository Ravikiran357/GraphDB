package global;
import java.io.*;

/*  File NID.java   */
/**
 * class NID
 */

public class NID extends RID{
	
	/**
	 * public int slotNo
	 */
	public int slotNo;

	/**
	 * public PageId pageNo
	 */
	public PageId pageNo = new PageId();

	/**
	 * default constructor of class
	 */
	public NID() {
	}

	/**
	 * constructor of class
	 */
	public NID(PageId pageno, int slotno) {
		pageNo = pageno;
		slotNo = slotno;
	}

	/**
	 * make a copy of the given rid
	 */
	public void copyNid(NID nid) {
		super.copyRid(nid);
	}

}

