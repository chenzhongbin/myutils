package mine.chen.me;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import mine.chen.util.DbcpUtil;

/**
 * 统计表记录数
 * @author chen
 *
 */
public class CountRows {
	private static final String SQL_GET_TABLES="SELECT TABLE_NAME,OWNER FROM ALL_TABLES WHERE 1=1 {0} order by owner,table_name";
	private static final String SQL_COUNT="select count(1) from {0}.{1}";
	private static final String MSG="{0},{1},{2}";
	private String[] owners;
	private Connection conn;
	
	public CountRows(Connection conn,String... owners){
		this.conn=conn;
		this.owners=owners;
	}
	
	public void execute(){
		try {
			StringBuilder tip=new StringBuilder();
			if(owners.length>0){
				tip.append(" and owner in(");
				for(int i=0;i<owners.length;i++){
					if(i==0){
						tip.append("?");
					}else{
						tip.append(",?");
					}
				}
				tip.append(") ");
			}
			String sql_get_tables=MessageFormat.format(SQL_GET_TABLES, tip.toString());
			PreparedStatement psGetTables=conn.prepareStatement(sql_get_tables);
			if(owners.length>0){
				for(int i=0;i<owners.length;i++){
					psGetTables.setString(i+1, owners[i]);
				}
			}
			ResultSet rsGetTables=psGetTables.executeQuery();
			while(rsGetTables.next()){
				String tableName=rsGetTables.getString("TABLE_NAME");
				String owner=rsGetTables.getString("OWNER");
				String sql_count=MessageFormat.format(SQL_COUNT, owner,tableName);
				PreparedStatement psCount=conn.prepareStatement(sql_count);
				ResultSet rsCount=psCount.executeQuery();
				if(rsCount.next()){
					System.out.println(MessageFormat.format(MSG, owner,tableName,rsCount.getString(1)));
				}
				rsCount.close();
				psCount.close();
			}
			rsGetTables.close();
			psGetTables.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		try {
			Connection conn_172=DbcpUtil.getConnection("oracle_me_172");
			Connection conn_10=DbcpUtil.getConnection("oracle_me_10");
			Connection local=DbcpUtil.getConnection("oracle_me_localhost");
			CountRows countRows_172=new CountRows(conn_172,"ME");
			CountRows countRows_10=new CountRows(conn_10,"ME");
			CountRows countRows_local=new CountRows(local,"DIC");
			System.out.println("------------开始统计内网-------------");
			countRows_172.execute();
			System.out.println("------------结束统计内网-------------");
			System.out.println("------------开始统计外网-------------");
			countRows_10.execute();
			System.out.println("------------结束统计外网-------------");
			
			System.out.println("------------开始统计本地-------------");
//			countRows_local.execute();
			System.out.println("------------结束统计本地-------------");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
//			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_localhost");
		} 
	}
}
