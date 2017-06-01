package utils;
import java.io.File;


public class GetFilePathUtil {
	
	public static void getFilePath(File dir){
		if(dir.isDirectory()){
			File[] files=dir.listFiles();
			for(File f:files){
				getFilePath(f);
			}
		}else{
			String path=dir.getAbsolutePath();
			if(path.endsWith("hbm.xml")){
				System.out.println(path);
			}
		}
	}
	
	public static void main(String[] args) {
		File dir=new File("E:\\WONDERS_AIC_FGDJ\\0708_dagl_电子档案管理\\源代码\\dagl\\src\\com\\wondersgroup");
		getFilePath(dir);
	}
}
