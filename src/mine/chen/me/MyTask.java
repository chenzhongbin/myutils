package mine.chen.me;

import java.sql.Connection;
import java.text.MessageFormat;

import mine.chen.util.DbcpUtil;

public class MyTask {
	
	private static void backUpMeTables(){
		try {
			String SQL="SELECT T.*,''10'' BAK_TAG FROM {0} T";
			Connection srcConn_10 = DbcpUtil.getConnection("oracle_me_10");
			Connection srcConn_172 = DbcpUtil.getConnection("oracle_me_172");
			Connection desConn = DbcpUtil.getConnection("oracle_me_localhost");
			String[] tables=new String[]{
				"ME_ANNL_INFO",
				"ME_AUDIT_NODE",
				"ME_AUDIT_OPNN",
				"ME_DROP_APP",
				"ME_HIRED",
				"ME_INVEST_PLAN",
				"ME_INVEST_PLAN_FUND",
				"ME_LOAN_FOLLOW",
				"ME_MATERIAL",
				"ME_NOTICE_DISSENT",
				"ME_PAY_SUBSIDY_LOAN",
				"ME_SUPPORT_INFO"
			};
			for(String table:tables){
				System.out.println("开始备份["+table+"]");
				TableSync task = new TableSync(
				MessageFormat.format(SQL, "ME."+table),
				"ME_BAK."+table, 
				srcConn_10, desConn);
				task.execute();
			}		
		} catch (Exception e) {
			DbcpUtil.rollback("oracle_me_localhost");
			e.printStackTrace();
		} finally {
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_localhost");
		}
	}
	
	public static void main(String[] args) {
//		backUpMeTables();//备份指定表
		backupMeInfo();//备份内外网的me_info表内容
	}
	
	private static final String SQL_TMPLATE="SELECT T.*,SYSDATE BAK_DATE FROM {0} T";
	
	public static void backupMeInfo() {
		try {
			Connection srcConn_10 = DbcpUtil.getConnection("oracle_me_10");
			Connection srcConn_172 = DbcpUtil.getConnection("oracle_me_172");
			Connection desConn = DbcpUtil.getConnection("oracle_me_localhost");
			
			System.out.println("开始备份me_number_limit");
			TableSync syn_me_number_limit = new TableSync(
					MessageFormat.format(SQL_TMPLATE, "ME.me_number_limit"),
					"ME_BAK.me_number_limit_BAK", 
					srcConn_10, desConn);
			syn_me_number_limit.execute();
			
			String SQL_ME_INFO="SELECT T.*,''{1}'' TAG FROM {0} T";
			System.out.println("开始备份me_info[172]");
			TableSync syn_me_info_172 = new TableSync(
					MessageFormat.format(SQL_ME_INFO, "ME.me_info","172"),
					"ME_BAK.me_info_BAK", 
					srcConn_172, desConn);
			syn_me_info_172.execute();
			
			System.out.println("开始备份me_info[10]");
			TableSync syn_me_info_10 = new TableSync(
					MessageFormat.format(SQL_ME_INFO, "ME.me_info","10"),
					"ME_BAK.me_info_BAK", 
					srcConn_10, desConn);
			syn_me_info_10.execute();
			
		} catch (Exception e) {
			DbcpUtil.rollback("oracle_me_localhost");
			e.printStackTrace();
		} finally {
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_localhost");
		}
	}
}
