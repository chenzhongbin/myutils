package utils;



public class TestCase {
	
	public static double test(double x,int d,double maybe){
		double val=maybe*maybe;
		System.out.println(val);
		if(val>x){
			return test(x,d,maybe/2);
		}else{
			return test(x,d,(maybe+x)/2);
		}
	}
	
	public static void main(String[] args) {
		System.out.println(test(3d,1,1d));
	}
	
}
