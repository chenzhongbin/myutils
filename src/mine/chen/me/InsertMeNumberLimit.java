package mine.chen.me;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mine.chen.util.DbcpUtil;
import mine.chen.util.TextUtil;

public class InsertMeNumberLimit {
	

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
    
	private static void executeInsertMeNumberLimit(Connection srcConn) throws SQLException {
		try {
			StringBuffer sql=TextUtil.readToBuffer(
					InsertMeNumberLimit.class.getResourceAsStream("insert_me_number_limit.sql"));
			String sqlFind="select * from me.me_number_limit "
					+ " where limit_year=? and organ_id=? and dept_id=? and is_sum=? ";
			PreparedStatement psFind=srcConn.prepareStatement(sqlFind);
			PreparedStatement psQuery=srcConn.prepareStatement(sql.toString());
			PreparedStatement psInsert=srcConn.prepareStatement(
					"insert into me.me_number_limit "
					+ "	(id,organ_id,dept_id,limit_year,max_number,update_date,is_sum,update_person,if_net) "
					+ " values(sys_guid(),?,?,?,?,sysdate,'1','chenzhongbin','1') ");
			ResultSet rsQuery=psQuery.executeQuery();
			int insertCount=0;
			while(rsQuery.next()){
				String limitYear=rsQuery.getString("LIMIT_YEAR");
				String organId=rsQuery.getString("ORGAN_ID");
				String deptId=rsQuery.getString("DEPT_ID");
				String isSum=rsQuery.getString("IS_SUM");
				int maxNumber=rsQuery.getInt("MAX_NUMBER");
				
				if(isSum==null||"1".equals(isSum)){
					continue;
				}
				if(deptId==null||"".equals(deptId)){
					continue;
				}
				psFind.setString(1, limitYear);
				psFind.setString(2, organId);
				psFind.setString(3, deptId);
				psFind.setString(4, "1");
				ResultSet rs=psFind.executeQuery();
				if(rs.next()){
					continue;
				}
				rs.close();
				
				psInsert.setString(1,organId);
				psInsert.setString(2,deptId);
				psInsert.setString(3,limitYear);
				psInsert.setInt(4,maxNumber);
				psInsert.addBatch();
				insertCount++;
				if(insertCount%20==0){
					psInsert.executeBatch();
					srcConn.commit();
				}
			}
			psInsert.executeBatch();
			srcConn.commit();
			System.out.println("------共插入：["+insertCount+"]条记录------");
			rsQuery.close();
			psInsert.close();
			psQuery.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} 
	}
	
    public static void main(String[] args) {
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_10");
			System.out.println("开始插入缺失扶持数量记录...");
			executeInsertMeNumberLimit(srcConn);
		} catch (Exception e) {
			e.printStackTrace();
			DbcpUtil.rollback("oracle_me_10");
		} finally{
			DbcpUtil.release("oracle_me_10");
		}
    }
    
}
