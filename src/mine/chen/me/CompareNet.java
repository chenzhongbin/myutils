package mine.chen.me;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mine.chen.util.DbcpUtil;

/**
 * 查询外网有，内网无的记录
 * @author chen
 *
 */
public class CompareNet {
	
	public static void main(String[] args) {
		try {
			Connection conn_172=DbcpUtil.getConnection("oracle_me_172");
			Connection conn_10=DbcpUtil.getConnection("oracle_me_10");
			Connection conn_localhost=DbcpUtil.getConnection("oracle_me_localhost");
			PreparedStatement ps_172=conn_172.prepareStatement("select * from me.me_info where me_id=? ");
			PreparedStatement ps_10=conn_10.prepareStatement("select * from me.me_info");
			PreparedStatement ps_localhost=conn_localhost.prepareStatement(
					"insert into me_bak.me_info_COMPARE"
					+ "(me_id,"
					+ "	FLOW_STATUS,FLOW_STATUS_NET,"
					+ "	ETPS_NAME,TAG,"
					+ "	if_net,if_net_net,"
					+ "	VALIDITY,VALIDITY_NET)"
					+ " values(?,?,?,?,?,?,?,?,?)");
			ResultSet rs_10=ps_10.executeQuery();
			while(rs_10.next()){
				String me_id_10=rs_10.getString("me_id");
				System.out.println(me_id_10);
				String flow_status_10=rs_10.getString("flow_status");
				String etps_name_10=rs_10.getString("etps_name");
				String if_net_10=rs_10.getString("if_net");
				String validity_10=rs_10.getString("validity");
				ps_172.setString(1, me_id_10);
				ResultSet rs_172=ps_172.executeQuery();
				//外网有，内网无
				if(!rs_172.next()){
					ps_localhost.setString(1, me_id_10);
					ps_localhost.setString(2, null);
					ps_localhost.setString(3, flow_status_10);
					ps_localhost.setString(4, etps_name_10);
					System.out.println(etps_name_10);
					ps_localhost.setString(5, "外网有内网无");
					ps_localhost.setString(6, null);
					ps_localhost.setString(7, if_net_10);
					ps_localhost.setString(8, null);
					ps_localhost.setString(9, validity_10);
					ps_localhost.executeUpdate();
				}
				rs_172.close();
			}
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

