package tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import diskmgr.GraphDB;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import nodeheap.HFBufMgrException;
import nodeheap.HFDiskMgrException;
import nodeheap.HFException;
import nodeheap.InvalidSlotNumberException;
import nodeheap.InvalidTupleSizeException;
import nodeheap.NodeHeapfile;

class BatchDriver implements GlobalConst{
	protected String dbpath;
	
	public void menu(){
		System.out.println("-------------------------- MENU ------------------");
		System.out.println("\n\n[0] Batch Node Insert");
		System.out.println("[1] Batch Node Delete");

		System.out.println("\n[2] Batch Edge Insert");
		System.out.println("[3] Batch Edge Delete");
		System.out.println("[4] Simple Node Query");
		System.out.println("[5] Simple Edge Query");
		System.out.println("[6] Quit");
	}
	
	public void runTests() {
		Random random = new Random();
		dbpath = "BTREE" + random.nextInt() + ".minibase-db";
		String logpath = "BTREE" + random.nextInt() + ".minibase-log";

		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 5000, "Clock");
		System.out.println("\n" + "Running " + " tests...." + "\n");

		int keyType = AttrType.attrInteger;

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		// This step seems redundant for me. But it's in the original
		// C++ code. So I am keeping it as of now, just in case I
		// I missed something
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}


		// Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		System.out.print("\n" + "..." + " Finished ");
		System.out.println(".\n\n");

}
	
	public void runAlltests(int choice, String filename, String graphDBName) throws Exception {
		switch(choice){
		case 0:
			GraphDB db = SystemDefs.JavabaseDB;//new GraphDB(0);
			NodeHeapfile nhf = SystemDefs.JavabaseDB.nodeHeapfile;
			
			BatchNodeInsert batchNodeInsert = new BatchNodeInsert();
			int k = 0;
			try{

			for (String line : Files.readAllLines(Paths.get(filename),StandardCharsets.US_ASCII)) {
				System.out.println("record number is "+k);
				batchNodeInsert.doSingleBatchNodeInsert(line, nhf, db);
				if(k == 12){
					System.out.println("record number is "+k);
				}
				k++;
			}
			}
			catch (Exception e){
				System.exit(1);
			}
			System.out.println("Node count: " + db.getNodeCnt() + "\nEdge count:" + db.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
			break;
		
		case 1:
			
			BatchNodeDelete batchNodeDelete = new BatchNodeDelete();
			for (String line : Files.readAllLines(Paths.get(filename),StandardCharsets.US_ASCII)) {
				batchNodeDelete.doSingleBatchNodeDelete(line.trim());
			}
			System.out.println("Node count: " + SystemDefs.JavabaseDB.getNodeCnt() + "\nEdge count:" + SystemDefs.JavabaseDB.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
			break;
		case 2:
			BatchEdgeInsert batchEdgeInsert = new BatchEdgeInsert();
			String[] edgeVals = new String[4];
			
			for (String line : Files.readAllLines(Paths.get(filename),StandardCharsets.US_ASCII)) {
				line = line.trim();
				edgeVals = line.split(" ");
				batchEdgeInsert.doSingleBatchEdgInsert(edgeVals[0], edgeVals[1], edgeVals[2], edgeVals[3]);
			}
			System.out.println("Node count: " + SystemDefs.JavabaseDB.getNodeCnt() + "\nEdge count:" + SystemDefs.JavabaseDB.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
			break;
		
		case 3:	
			BatchEdgeDelete batchEdgeDelete = new BatchEdgeDelete();
			String[] edgeValsDel = new String[4];
			
			for (String line : Files.readAllLines(Paths.get(filename),StandardCharsets.US_ASCII)) {
				line = line.trim();
				edgeValsDel = line.split(" ");
				batchEdgeDelete.doSingleBatchEdgeDelete(edgeValsDel[0], edgeValsDel[1], edgeValsDel[2]);
			}
			System.out.println("Node count: " + SystemDefs.JavabaseDB.getNodeCnt() + "\nEdge count:" + SystemDefs.JavabaseDB.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
		
		default:
			break;
	}
}
}
	
	class GetStuff {
		GetStuff() {
		}
	
		public static int getChoice() {
	
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			int choice = -1;
	
			try {
				choice = Integer.parseInt(in.readLine());
			} catch (NumberFormatException e) {
				return -1;
			} catch (IOException e) {
				return -1;
			}
	
			return choice;
		}
	}
	
	
public class BatchTest implements GlobalConst{
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	public static void main(String[] args) {
		int choice = 0;
		
		try {
			BatchDriver bttest = new BatchDriver();
			bttest.runTests();
			if (args.length > 0) {
				boolean status = OK;
				String file = args[0];
				String graphDB = args[1];
				while (choice != 6) {
					bttest.menu();
					try {
						choice = GetStuff.getChoice();
						bttest.runAlltests(choice, file, graphDB);
					}catch (Exception e) {
						e.printStackTrace();
						System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
						System.out.println("       !!         Something is wrong                    !!");
						System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
						System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					}
				}
			}
			else {
				System.out.println("No inputs given\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error encountered during buffer manager tests:\n");
			Runtime.getRuntime().exit(1);
		}
	}
}

