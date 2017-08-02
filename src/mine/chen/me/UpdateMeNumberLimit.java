package mine.chen.me;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mine.chen.util.DbcpUtil;

public class UpdateMeNumberLimit {
	
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

    private static void executeSql(Connection conn,String sql) throws SQLException{
    	PreparedStatement psUpdate=null;
    	try {
			psUpdate=conn.prepareStatement(sql);
			psUpdate.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e2) {
			}
			e.printStackTrace();
			throw e;
		} finally{
			try {
				if(psUpdate!=null)
					psUpdate.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
    	
    }
    
	private static void executeUpdateMeNumberLimit(Connection srcConn) throws SQLException {
		try {
			StringBuffer sql=readToBuffer(UpdateMeNumberLimit.class.getResourceAsStream("update_me_number_limit.sql"));
			PreparedStatement psCount=srcConn.prepareStatement(sql.toString());
			PreparedStatement psUpdate=srcConn.prepareStatement("update me.me_number_limit set apply_number=? where id=?");
			ResultSet rsCount=psCount.executeQuery();
			int index=0;
			while(rsCount.next()){
				int count=rsCount.getInt("COUNT");
				String id=rsCount.getString("ID");
				psUpdate.setInt(1, count);
				psUpdate.setString(2, id);
				psUpdate.addBatch();
				index++;
				if(index%20==0){
					psUpdate.executeBatch();
					srcConn.commit();
				}
			}
			psUpdate.executeBatch();
			srcConn.commit();
			rsCount.close();
			psUpdate.close();
			psCount.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} 
	}
	
    public static void main(String[] args) {
    	while(true){
    		try {
    			Connection srcConn=DbcpUtil.getConnection("oracle_me_10");
    			
    			long startTime=System.currentTimeMillis();
    			System.out.println("***开始执行定时任务***"+new Date(System.currentTimeMillis()));
    			
    			System.out.println("开始校正扶持数量...");
    			executeUpdateMeNumberLimit(srcConn);
    			
//    			System.out.println("开始更新me_info.app_date...");
//    			String sql_update_app_date=readToBuffer(UpdateMeNumberLimit.class.getResourceAsStream("update_app_date.sql")).toString();
//    			executeSql(srcConn, sql_update_app_date);
    			
    			long endTime=System.currentTimeMillis();
    			System.out.println("***结束定时任务*** [耗时："+(endTime-startTime)+"]");
    			
				Thread.sleep(60000);
			} catch (Exception e) {
				e.printStackTrace();
				DbcpUtil.rollback("oracle_me_10");
				//break;
			} finally{
				DbcpUtil.release("oracle_me_10");
			}
    	}
    }
    
}
