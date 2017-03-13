package zIndex;

import btree.*;
import global.Descriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class ZTreeFile extends BTreeFile{

	public ZTreeFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {
		super(filename);
		// TODO Auto-generated constructor stub
	}


	public ArrayList<DescriptorKey> zTreeFileScan() throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException, ScanIteratorException {
		BTFileScan scan = this.new_scan(null, null);
		ArrayList<DescriptorKey> retDescriptors = new ArrayList<DescriptorKey>();
		while(true) {
			KeyDataEntry entry = scan.get_next();
			if (entry == null){
				break;
			}
			DescriptorKey keyEntry = (DescriptorKey) entry.key;
			retDescriptors.add(keyEntry);
		}
		return retDescriptors;
	}

//	public ArrayList<Descriptor> zTreeRangeScan(Descriptor key, int distance) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException {

	public static void zTreeRangeScan(Descriptor key, int distance) throws PinPageException, KeyNotMatchException, IteratorException, IOException, ConstructPageException, UnpinPageException {
		ArrayList<Descriptor> retDescriptors = new ArrayList<Descriptor> ();
		//First find the lowerbound for the given key.
		int lowKeyVal[] = new int[5];
		lowKeyVal[0] = key.get(0) - distance;
		lowKeyVal[1] = key.get(1) - distance;
		lowKeyVal[2] = key.get(2) - distance;
		lowKeyVal[3] = key.get(3) - distance;
		lowKeyVal[4] = key.get(4) - distance;

		//Make sure that the lowKey does not cross 0;
		for(int i = 0;i<lowKeyVal.length;i++){
			if(lowKeyVal[i]<0){
				lowKeyVal[i] = 0;
			}
		}
		System.out.println("lowKeyVals are" + lowKeyVal[0] + " "+
				lowKeyVal[1]+" "+
				lowKeyVal[2]+ " "+
				lowKeyVal[3]+ " "+
				lowKeyVal[4]);
		int highKeyVal[] = new int[5];
		highKeyVal[0] = key.get(0) + distance;
		highKeyVal[1] = key.get(1) + distance;
		highKeyVal[2] = key.get(2) + distance;
		highKeyVal[3] = key.get(3) + distance;
		highKeyVal[4] = key.get(4) + distance;

		//Make sure that the highKey does not cross the maxInt
		for(int i = 0;i<highKeyVal.length;i++){
			if(highKeyVal[i]<0){
				highKeyVal[i] = Integer.MAX_VALUE;
			}
		}
		System.out.println("highKeyVal are" + highKeyVal[0] + " "+
				highKeyVal[1]+" "+
				highKeyVal[2]+ " "+
				highKeyVal[3]+ " "+
				highKeyVal[4]);

		Descriptor lowKey = new Descriptor();
		Descriptor highKey = new Descriptor();
		lowKey.set(lowKeyVal[0],lowKeyVal[1],lowKeyVal[2],lowKeyVal[3],lowKeyVal[4]);
		highKey.set(highKeyVal[0], highKeyVal[1], highKeyVal[2], highKeyVal[3], highKeyVal[4]);


		ArrayList<String> allPossible = new ArrayList<String>();

		System.out.println("all possible are ");
		for(int i = lowKeyVal[0]; i< highKeyVal[0];i++){
			for(int j = lowKeyVal[1]; j< highKeyVal[1];j++) {
				for(int k = lowKeyVal[2]; k< highKeyVal[2];k++) {
					for(int l = lowKeyVal[3]; l< highKeyVal[3];l++) {
						for(int m = lowKeyVal[4]; m< highKeyVal[4];m++) {
							//System.out.println(i+" "+ j+" "+ k+" "+l+" "+m);
							Descriptor desc = new Descriptor();
							desc.set(i,j,k,l,m);
							DescriptorKey descKey = new DescriptorKey(desc);
							String descStr = descKey.desc_str;
							allPossible.add(descStr);
							//System.out.println(" " + descStr);
						}
					}
				}
			}
		}
		System.out.println("allPossible size is"+allPossible.size());


		//Sort all possible zorders
		Collections.sort(allPossible);
		for(int r =0; r< allPossible.size(); r++){
			int zorder = (int)(long)DescriptorKey.getZorder(allPossible.get(r));

			//System.out.print(zorder+ "  ");
		}
		int a = 0;
		//System.exit(1);
		while( a< allPossible.size()){
			int zorder = (int)(long)DescriptorKey.getZorder(allPossible.get(a));
			//System.out.println("zorder is "+zorder);
			//Check if the next element exists
			//System.out.print(zorder+" ");

			if(a+1< allPossible.size()){
				//peek next
				int nextZorder = (int)(long)DescriptorKey.getZorder(allPossible.get(a+1));
				if(nextZorder != zorder+1){
					int za=1;
					//System.out.println();

				}
			}
			a+=1;
		}

		//BTFileScan scan = this.new_scan(null, null);
		//return retDescriptors;
	}

	public static void main(String[] args) throws KeyNotMatchException, IteratorException, IOException, PinPageException, ConstructPageException, UnpinPageException {
		System.out.println("haga summane");
		Descriptor key = new Descriptor();
		key.set(5,4,3,7,3);
		zTreeRangeScan(key, 12);

	}


}