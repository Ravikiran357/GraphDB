package global;

public class Descriptor {
	int value [];
	value = new int[5];
	value[0] = 0;
	value[1] = 0;
	value[2] = 0;
	value[3] = 0;
	value[4] = 0;
void set(int value0, int value1, int value2, int value3, int value4) {
	value[0] = value0;
	value[1] = value1;
	value[2] = value2;
	value[3] = value3;
	value[4] = value4;
}
int get(int idx) {
	return value[idx];
}
double equal (Descriptor desc) {
	for(int i=0;i<5;i++){
		if(this.value[i] != desc.value[i]){
			return 0;
		}
	}
	return 1;
}
double distance (Descriptor desc) {
	double sum =0;
	for(int i=0;i<5;i++){
		sum += (this.value[i]-desc.value[i])*(this.value[i]-desc.value[i]);
	}
	return Math.sqrt(sum);
	}
}
