package mine.chen.me;

import java.sql.Connection;

import mine.chen.util.DbcpUtil;

public class CheckTask {
	
	public static void main(String[] args) {

		try {
			String SQL="SELECT T.*,''10'' BAK_TAG FROM {0} T";
			Connection srcConn_10 = DbcpUtil.getConnection("oracle_me_10");
			Connection srcConn_172 = DbcpUtil.getConnection("oracle_me_172");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_172");
		}
	
	}
	
}
