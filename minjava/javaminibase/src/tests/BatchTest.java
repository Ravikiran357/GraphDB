package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;

import btree.BT;
import bufmgr.BufMgrException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.GraphDB;
import diskmgr.PCounter;
import global.GlobalConst;
import global.SystemDefs;
import nodeheap.NodeHeapfile;

class BatchDriver implements GlobalConst {
	protected String dbpath = "data.minibase-db";
	private int numBuf = NUMBUF;
	
	public void exitClean() throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, HashOperationException, PagePinnedException, PageNotFoundException, BufMgrException, IOException{
		//close index files
		SystemDefs.JavabaseDB.nodeDescriptorIndexFile.close();
		SystemDefs.JavabaseDB.nodeLabelIndexFile.close();
		SystemDefs.JavabaseDB.edgeLabelIndexFile.close();
		SystemDefs.JavabaseDB.edgeWeightIndexFile.close();
		
		SystemDefs.JavabaseBM.flushAllPages();
		
		SystemDefs.JavabaseDB.closeDB();
		
	}
	
	public void init(){
		System.out.println("\n" + "Running " + " tests...." + "\n");
		File f = new File(dbpath);
		if(f.exists()) { 
			SystemDefs.MINIBASE_RESTART_FLAG = true;
		}
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, numBuf, "Clock");		
	}
	
	public void clearFiles(){
		System.out.println("\n" + "Clearing db files.\n");

		Random random = new Random();
		String logpath = "BTREE" + random.nextInt() + ".minibase-log";

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

	public void menu() {
		System.out.println("-------------------------- MENU ------------------");
		System.out.println("\n\n[0] Batch Node Insert");
		System.out.println("[1] Batch Node Delete");

		System.out.println("\n[2] Batch Edge Insert");
		System.out.println("[3] Batch Edge Delete");
		System.out.println("[4] Simple Node Query");
		System.out.println("[5] Simple Edge Query");
		System.out.println("[6] Quit");
	}

	public void runAllTests(int choice) throws Exception {
		Scanner in = new Scanner(System.in);
		switch (choice) {
		case 0:
			System.out.println("Nodefile name: ");
			String filename = in.nextLine();
			System.out.println("Graphdb name: ");
			dbpath = in.nextLine();
			GraphDB db = SystemDefs.JavabaseDB;// new GraphDB(0);
			NodeHeapfile nhf = SystemDefs.JavabaseDB.nodeHeapfile;

			BatchNodeInsert batchNodeInsert = new BatchNodeInsert();

			for (String line : Files.readAllLines(Paths.get(filename), StandardCharsets.US_ASCII)) {
				batchNodeInsert.doSingleBatchNodeInsert(line, nhf, db);

			}
			System.out.println("Node count: " + db.getNodeCnt() + "\nEdge count:" + db.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
			break;
		case 1:
			System.out.println("Nodefile name: ");
			filename = in.nextLine();
			System.out.println("Graphdb name: ");
			dbpath = in.nextLine();
			BatchNodeDelete batchNodeDelete = new BatchNodeDelete();
			for (String line : Files.readAllLines(Paths.get(filename), StandardCharsets.US_ASCII)) {
				batchNodeDelete.doSingleBatchNodeDelete(line.trim());
			}
			System.out.println("Node count: " + SystemDefs.JavabaseDB.getNodeCnt() + "\nEdge count:"
					+ SystemDefs.JavabaseDB.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
			break;

		case 2:
			System.out.println("Edgefile name: ");
			String edgeFile = in.nextLine();
			System.out.println("Graphdb name: ");
			dbpath = in.nextLine();
			BatchEdgeInsert batchEdgeInsert = new BatchEdgeInsert();
			String[] edgeVals = new String[4];
			int i = 0;
			for (String line : Files.readAllLines(Paths.get(edgeFile), StandardCharsets.US_ASCII)) {
				line = line.trim();
				edgeVals = line.split(" ");
				batchEdgeInsert.doSingleBatchEdgInsert(edgeVals[0], edgeVals[1], edgeVals[2], edgeVals[3]);
				System.out.println("Edges inserted : " + i++);
			}
			System.out.println("Node count: " + SystemDefs.JavabaseDB.getNodeCnt() + "\nEdge count:"
					+ SystemDefs.JavabaseDB.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);

			break;

		case 3:
			System.out.println("Edgefile name: ");
			edgeFile = in.nextLine();
			System.out.println("Graphdb name: ");
			dbpath = in.nextLine();
			BatchEdgeDelete batchEdgeDelete = new BatchEdgeDelete();
			String[] edgeValsDel = new String[4];
			int k = 0;
			for (String line : Files.readAllLines(Paths.get(edgeFile), StandardCharsets.US_ASCII)) {
				line = line.trim();
				System.out.println("deleting edge " + k++);
				edgeValsDel = line.split(" ");
				batchEdgeDelete.doSingleBatchEdgeDelete(edgeValsDel[0], edgeValsDel[1], edgeValsDel[2]);
			}
			System.out.println("Node count: " + SystemDefs.JavabaseDB.getNodeCnt() + "\nEdge count:"
					+ SystemDefs.JavabaseDB.getEdgeCnt());
			System.out.println("No of pages read" + PCounter.rcounter + "\nNo of pages written" + PCounter.wcounter);
			break;
		case 4:
			System.out.println("Enter Graphdb name: ");
			dbpath = in.nextLine();
			System.out.println("Enter Numbuf: ");
			numBuf = in.nextInt();
			System.out.println("Enter Query Type:");
			int qtype = in.nextInt();
			System.out.println("With(1) or Without index(0):");
			int index = in.nextInt();
			String[] args = new String[2];
			switch(qtype) {
			case 0: break;
			case 1: break;
			case 2: System.out.println("Enter Descriptor as a csv val: ");
					args[0] = in.next();
					break;
			case 3: System.out.println("Enter Descriptor as a csv val: ");
					args[0] = in.next();
					System.out.println("Enter Distance: ");
					args[1] = in.next();
					break;
			case 4:System.out.println("Enter Label: ");
				   args[0] = in.next();
				   break;
			case 5:System.out.println("Enter Descriptor as a csv val: ");
				   args[0] = in.next();
			       System.out.println("Enter Distance: ");
			       args[1] = in.next();
			       break;
			default:
		}
		    NodeQuery nq = new NodeQuery();
			nq.evaluate(qtype, index, args);
			break;
		case 5:
			System.out.println("Enter Graphdb name: ");
			dbpath = in.nextLine();
			System.out.println("Enter Numbuf: ");
			numBuf = in.nextInt();
			System.out.println("Enter Query Type:");
			qtype = in.nextInt();
			System.out.println("With(1) or Without index(0):");
			index = in.nextInt();
			args = new String[2];
			switch(qtype) {
			case 0: break;
			case 1: break;
			case 2: break;
			case 3:	break;
			case 4: break;
			case 5: System.out.println("Enter lower edge weight:");
					args[0] = in.next();
					System.out.println("Enter upper edge weight:");
					args[1] = in.next();
					break;
			case 6: break;
			default:
		}
		    EdgeQuery eq = new EdgeQuery();
		    eq.evaluate(qtype, index, args);
			break;
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

public class BatchTest implements GlobalConst {
	public static void main(String[] args) {
		int choice = 0;

		try {
			BatchDriver bttest = new BatchDriver();
			bttest.init();
			bttest.menu();
			while ((choice = GetStuff.getChoice()) != 6) {
				SystemDefs.JavabaseDB.resetPageCounter();
				try {
					bttest.runAllTests(choice);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					System.out.println("       !!         Something is wrong                    !!");
					System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
					System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				}
				bttest.menu();
			}
			
			//choice 6, exit.
			bttest.exitClean();

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error encountered during tests:\n");
			Runtime.getRuntime().exit(1);
		}
	}
}
