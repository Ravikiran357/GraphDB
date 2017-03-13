package zIndex;

import btree.KeyClass;
import global.Descriptor;

public class DescriptorKey extends KeyClass{

	private Descriptor key;
	
	public DescriptorKey(Descriptor desc){
		this.key = desc;
		desc_str = this.getZVal(desc);
	}
	public String desc_str;
	
	public String toString(){
		StringBuilder result = new StringBuilder();
		result.append(key.get(0));
		result.append(key.get(1));
		result.append(key.get(2));
		result.append(key.get(3));
		result.append(key.get(4));
		return result.toString();
	}
	
	public Descriptor getKey() {
		return key;
	}
	
	public String getDescString() {
		return desc_str;
	}


	public void setKey(Descriptor key) {
		this.key = key;
	}

	
	private  String getZVal(Descriptor desc){
		StringBuilder result = new StringBuilder();
		
		int val0 = desc.get(0);
		int val1 = desc.get(1);
		int val2 = desc.get(2);
		int val3 = desc.get(3);
		int val4 = desc.get(4);
		
		for(int i = 0; i < 32; i++){
			//TODO take out prefix of 0's.
			result.append(val4%2);
			result.append(val3%2);
			result.append(val2%2);
			result.append(val1%2);
			result.append(val0%2);
			
			val0 /= 2;
			val1 /= 2;
			val2 /= 2;
			val3 /= 2;
			val4 /= 2;
		}
		
		return result.reverse().toString();
		
	}
	
	public static void main(String[] args){
		Descriptor desc = new Descriptor();
		desc.set(1,2,3,4,5);
		System.out.println(getZVal(desc));
		
	}
	


}
