package mine.chen.me;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mine.chen.util.DbcpUtil;

public class CopyData {
	
	public static void main(String[] args) {
		try {
			Connection conn_172=DbcpUtil.getConnection("oracle_me_172");
			Connection conn_10=DbcpUtil.getConnection("oracle_me_10");
			Connection conn_localhost=DbcpUtil.getConnection("oracle_me_localhost");
			PreparedStatement ps_172=conn_172.prepareStatement("select * from me.me_info where app_type='1'");
			PreparedStatement ps_10=conn_10.prepareStatement("select * from me.me_info where me_id=? and app_type='1'");
			PreparedStatement ps_localhost=conn_localhost.prepareStatement("insert into me.me_info_COMPARE(me_id,FLOW_STATUS,FLOW_STATUS_NET,ETPS_NAME,TAG,if_net,if_net_net) values(?,?,?,?,?,?,?)");
			ResultSet rs_172=ps_172.executeQuery();
			while(rs_172.next()){
				String me_id=rs_172.getString("me_id");
				String flow_status=rs_172.getString("flow_status");
				String etps_name=rs_172.getString("etps_name");
				String if_net=rs_172.getString("if_net");
				ps_10.setString(1, me_id);
				ResultSet rs_10=ps_10.executeQuery();
				if(rs_10.next()){
					String flow_status_10=rs_10.getString("flow_status");
					String if_net_10=rs_10.getString("if_net");
					if(flow_status==flow_status_10||(flow_status!=null&&flow_status.equals(flow_status_10))){
						
					}else{
						ps_localhost.setString(1, me_id);
						ps_localhost.setString(2, flow_status);
						ps_localhost.setString(3, flow_status_10);
						ps_localhost.setString(4, etps_name);
						System.out.println(etps_name);
						ps_localhost.setString(5, "流程状态不同1219_1");
						ps_localhost.setString(6, if_net);
						ps_localhost.setString(7, if_net_10);
						ps_localhost.executeUpdate();
					}
				}
				rs_10.close();
			}
			rs_172.close();
			
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

