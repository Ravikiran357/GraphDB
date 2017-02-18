package global;

/**
 * Created by revu on 2/17/17.
 */
public class task1TestMain {
    public static void main(String[] args){
        Descriptortype desc1 = new Descriptortype();
        desc1.set(0,1,2,3,4);
        Descriptortype desc2 = new Descriptortype();
        desc2.set(0,0,0,0,0);
        System.out.println("distance is "+desc1.distance(desc2));
        System.out.println("equals is "+ desc1.equal(desc2));
        desc1.set(0,0,0,0,0);
        System.out.println("equals is "+ desc1.equal(desc2));

    }
}
