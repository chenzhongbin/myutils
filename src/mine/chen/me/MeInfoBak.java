package mine.chen.me;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import mine.chen.util.DbcpUtil;

/**
 * me_info备份
 * @author chen
 *
 */
public class MeInfoBak {
	
	public static void main(String[] args) {
		try {
			Connection conn_172=DbcpUtil.getConnection("oracle_me_172");
			Connection conn_10=DbcpUtil.getConnection("oracle_me_10");
			Connection conn_localhost=DbcpUtil.getConnection("oracle_me_localhost");
			PreparedStatement ps_172=conn_172.prepareStatement("select * from me.me_info");
			PreparedStatement ps_10=conn_10.prepareStatement("select * from me.me_info");
			Date oper_date=new Date(Calendar.getInstance().getTimeInMillis());//操作时间
			ResultSet rs_172=ps_172.executeQuery();
			ResultSet rs_10=ps_10.executeQuery();
			List<String> colNames=new ArrayList<String>();
			StringBuilder insert_sql=new StringBuilder();
			insert_sql.append("insert into me_bak.ME_INFO_BAK ( ");
			ResultSetMetaData metaData_172=rs_172.getMetaData();
			int cols=metaData_172.getColumnCount();
			for(int i=0;i<cols;i++){
				String colName=metaData_172.getColumnName(i+1);
				colNames.add(colName);
				insert_sql.append(colName+",");
			}
			insert_sql.append("BAK_DATE,TAG) values(");
			for(int i=0;i<cols;i++){
				insert_sql.append("?,");
			}
			insert_sql.append("?,?)");
			
			PreparedStatement ps_localhost=conn_localhost.prepareStatement(insert_sql.toString());
			System.out.println("***开始同步内网数据***");
			int count=0;
			while(rs_172.next()){
				for(int i=0;i<cols;i++){
					Object obj=rs_172.getObject(colNames.get(i));
					ps_localhost.setObject(i+1, obj);
				}
				ps_localhost.setDate(cols+1, oper_date);
				ps_localhost.setString(cols+2, "172");
				count++;
				ps_localhost.addBatch();
				if(count%100==0){
					ps_localhost.executeBatch();
				}
			}
			ps_localhost.executeBatch();
			rs_172.close();
			System.out.println("***开始同步外网数据***");
			while(rs_10.next()){
				for(int i=0;i<cols;i++){
					Object obj=rs_10.getObject(colNames.get(i));
					ps_localhost.setObject(i+1, obj);
				}
				ps_localhost.setDate(cols+1, oper_date);
				ps_localhost.setString(cols+2, "10");
				count++;
				ps_localhost.addBatch();
				if(count%100==0){
					ps_localhost.executeBatch();
				}
			}
			ps_localhost.executeBatch();
			rs_10.close();
			
			conn_localhost.commit();
		} catch (SQLException e) {
//			DbcpUtil.rollback("oracle_me_172");
//			DbcpUtil.rollback("oracle_me_10");
			DbcpUtil.rollback("oracle_me_localhost");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_localhost");
		} 
	}
	
}

