package mine.chen.me;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

public class DBUtil {
	
    private static StringBuffer readToBuffer(InputStream is) throws IOException {
    	StringBuffer buffer=new StringBuffer();
    	String line; // 用来保存每行读取的内容
        BufferedReader reader = null;
        try {
        	reader = new BufferedReader(new InputStreamReader(is));
        	line = reader.readLine(); // 读取第一行
        	while (line != null) { // 如果 line 为空说明读完了
        		buffer.append(line); // 将读到的内容添加到 buffer 中
        		buffer.append("\n"); // 添加换行符
        		line = reader.readLine(); // 读取下一行
        	}
        	reader.close();
        	return buffer;
		} catch (IOException e) {
			throw e;
		} finally{
			try {
				if(reader!=null) reader.close();
			} catch (IOException ex) {
			}
			try {
				if(is!=null) is.close();
			} catch (IOException ex) {
			}
		}
    }
    
	public static String ddlCreateBakTable(Connection conn) {
		try {
			StringBuffer sql=readToBuffer(DBUtil.class.getResourceAsStream("ddl_create_trigger.sql"));
			
			conn.prepareStatement(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String ddlCreateTrigger(Connection conn,String tableName){
		StringBuilder sql=new StringBuilder();
		sql.append("");
//		#TRIGGER_NAME#
//		#TABLE_NAME#
//		#COLUMNS#
//		#OLD_COLUMNS#
//		--create or replace TRIGGER TRI_ENTBASEINFO_OPER 
//		--BEFORE DELETE OR UPDATE ON ENT_BASE_INFO FOR EACH ROW
//		--DECLARE
//		--OPER_TYPE CHAR(1);
//		--BEGIN
		try {
			conn.prepareStatement(sql.toString());
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) {
		
	}
}
