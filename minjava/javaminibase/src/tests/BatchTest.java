package tests;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;

import diskmgr.GraphDB;
import diskmgr.PCounter;
import global.GlobalConst;
import global.SystemDefs;
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
	
	public void runQueryTests(int choice, String []args) {
		String[] args1 = new String[6];
		args1[0] = "a";
		args1[1] = "a";
		Scanner in = new Scanner(System.in);
		System.out.println("Qtype is :");
		args1[2] = in.next();
		System.out.println("With(1) or Without index(0)");
		args1[3] = in.next();
		if (choice == 4) {
			NodeQuery nq = new NodeQuery();
			if (Integer.parseInt(args1[2]) == 4) {
				System.out.println("Enter the label");
				args1[4] = in.next();
			}
			else if (Integer.parseInt(args1[2]) > 1) {
				System.out.println("Enter descriptor in csv");
				args1[4] = in.next();
				System.out.println("Enter distance");
				args1[5] = in.next();
			}

			nq.evaluate(args1);
		} else {
			if (Integer.parseInt(args1[2]) == 5) {
				System.out.println("Enter the lower bound");
				args1[4] = in.next();
				System.out.println("Enter the upper bound");
				args1[5] = in.next();
			}
			EdgeQuery eq = new EdgeQuery();
			eq.evaluate(args);
		}
	}
	
	public void runAllTests(int choice, String filename, String graphDBName, String edgeFile) throws Exception {
		GraphDB db = SystemDefs.JavabaseDB;
		
		switch(choice){
			case 0:
				NodeHeapfile nhf = SystemDefs.JavabaseDB.nodeHeapfile;
				BatchNodeInsert batchNodeInsert = new BatchNodeInsert();
				for (String line : Files.readAllLines(Paths.get(filename),StandardCharsets.US_ASCII)) {
					batchNodeInsert.doSingleBatchNodeInsert(line, nhf, db);
	
				}
				break;
	
			case 1:
				BatchNodeDelete batchNodeDelete = new BatchNodeDelete();
				for (String line : Files.readAllLines(Paths.get(filename),StandardCharsets.US_ASCII)) {
					batchNodeDelete.doSingleBatchNodeDelete(line.trim());
				}
				break;
				
			case 2:
				BatchEdgeInsert batchEdgeInsert = new BatchEdgeInsert();
				String[] edgeVals = new String[4];
				int i = 0;
				for (String line : Files.readAllLines(Paths.get(edgeFile),StandardCharsets.US_ASCII)) {
					line = line.trim();
					edgeVals = line.split(" ");
					batchEdgeInsert.doSingleBatchEdgInsert(edgeVals[0], edgeVals[1], edgeVals[2], edgeVals[3]);
					System.out.println("Edges inserted : " + i++);
				}
				break;
			
			case 3:	
				BatchEdgeDelete batchEdgeDelete = new BatchEdgeDelete();
				String[] edgeValsDel = new String[4];
				int k = 0;
				for (String line : Files.readAllLines(Paths.get(edgeFile),StandardCharsets.US_ASCII)) {
					line = line.trim();
					System.out.println("deleting edge "+k++);
					edgeValsDel = line.split(" ");
					batchEdgeDelete.doSingleBatchEdgeDelete(edgeValsDel[0], edgeValsDel[1], edgeValsDel[2]);
				}
				break;

			default:
				System.out.println("Invalid option given");
				break;
		}
		if (choice >= 0 && choice <= 3) {
			System.out.println("Node count: " + db.getNodeCnt() + "\nEdge count:" + db.getEdgeCnt());
			System.out.println("No of pages read: " + PCounter.rcounter + "\nNo of pages written: " + PCounter.wcounter);
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
	public static void main(String[] args) {
		int choice = 0;		
		try {
			BatchDriver bttest = new BatchDriver();
			bttest.runTests();
			if (args.length > 0) {
				String file = args[0];
				String graphDB = args[1];
				String edgeF = args[2];
				while (choice != 6) {
					bttest.menu();
					try {
						choice = GetStuff.getChoice();
						if (choice == 6)
							break;
						else if (choice > 3)
							bttest.runQueryTests(choice, args);
						else
							bttest.runAllTests(choice, file, graphDB, edgeF);
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
			System.out.println("Program exited succesfully");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error encountered during buffer manager tests:\n");
			Runtime.getRuntime().exit(1);
		}
	}
}



