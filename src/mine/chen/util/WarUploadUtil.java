package mine.chen.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Logger;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * @author chen
 * TODO JDK8正常访问SFTP，JDK7则不行
 */
public class WarUploadUtil {
	private static Logger logger=new Logger() {
		@Override
		public void log(int level, String message) {
			System.out.println("["+level+"]"+message);
		}
		@Override
		public boolean isEnabled(int level) {
			return true;
		}
	};
	
	public static void main(String[] args) {
		FileInputStream fis=null;
		try {
			fis=new FileInputStream(new File("‪‪d:"+File.pathSeparator+"aic_wars"+File.pathSeparator+"fgdj.war"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally{
			try {
				if(fis!=null)
					fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
    public static void main1(String[] args) {
    	JSch jsch=new JSch();
    	JSch.setLogger(logger);
    	Session session=null;
    	ChannelSftp sftp=null;
    	FileInputStream fis=null;
		try {
			session = jsch.getSession("weblogic", "10.253.254.40",22);
			session.setPassword("xdrcft56");
			Properties config = new Properties();
	        config.put("StrictHostKeyChecking", "no");
//	        config.put("kex", "diffie-hellman-group14-sha1");
	        session.setConfig(config);
//	        session.setTimeout(1500); 
			session.connect();
			sftp=(ChannelSftp) session.openChannel("sftp");
			sftp.connect();
			sftp.setFilenameEncoding("UTF-8");
			sftp.cd("/home/weblogic/Oracle/upload");
//			Vector<?> vector  = sftp.ls("/home/weblogic/Oracle/upload");
//			for(Object o:vector){
//				 if(o instanceof com.jcraft.jsch.ChannelSftp.LsEntry){
//					 String fileName=((com.jcraft.jsch.ChannelSftp.LsEntry) o).getFilename();
//					 System.out.println(fileName);
//				 }
//			}
//			sftp.rename("/home/weblogic/Oracle/upload/fgdj.war", "/home/weblogic/Oracle/upload/fgdj.war.xxx");
			fis=new FileInputStream(new File("‪‪d:"+File.pathSeparator+"AIC_WARS"+File.pathSeparator+"fgdj.war"));
			sftp.put(fis,"/home/weblogic/Oracle/upload/fgdj.war");
		} catch (JSchException e) {
			e.printStackTrace();
		} catch (SftpException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally{
			if(sftp!=null) sftp.quit();
			if(session!=null) session.disconnect();
			try {
				if(fis!=null)
					fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
