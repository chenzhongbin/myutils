package mine.chen.me;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import mine.chen.util.DbcpUtil;

/**
 * 表同步工具类
 * @author chen
 *
 */
public class TableSync {
	
	public static boolean IF_PRINT_MSG=true;
	public static int EXE_BATCH_ROWS=1000;//批量执行记录数
	
	private static final <T> void println(T msg) {
		if (IF_PRINT_MSG)
			System.out.println(msg);
	}
	public TableSync(){
		
	}
	public TableSync(String selectSql,String destTable, Connection srcConn, Connection destConn) {
		assertNull(selectSql,"select语句为空！");
		assertNull(destTable,"目标表名为空！");
		assertNull(srcConn,"源数据库连接为空！");
		assertNull(destConn,"目标数据库连接为空！");
		this.srcTable=null;
		this.destTable = destTable;
		this.srcConn = srcConn;
		this.destConn = destConn;
		this.selectSql = selectSql;
		TableSync.println("SELECT_SQL:"+this.selectSql);
	}

	public TableSync(String srcTable, String destTable, String pkName, Connection srcConn, Connection destConn) {
		assertNull(srcTable,"源表名为空！");
		assertNull(destTable,"目标表名为空！");
		assertNull(srcConn,"源数据库连接为空！");
		assertNull(destConn,"目标数据库连接为空！");
		this.srcTable = srcTable;
		this.destTable = destTable;
		this.pkName = pkName;
		this.srcConn = srcConn;
		this.destConn = destConn;
	}

	private String srcTable;
	private String destTable;
	private String pkName;//主键字段名
	private Connection srcConn;
	private Connection destConn;
	
	private String selectSql="";
	private String countSql="";
	private String countSrcSql="";
	
	private void init(){
		if(!isEmpty(srcTable)){
			selectSql="select * from "+srcTable;
		}
		countSrcSql="select count(1) AS COUNTS from ("+selectSql+") T";
		if(!isEmpty(pkName)){
			countSql="select count(1) AS COUNTS from "+destTable+" where "+pkName+"=?";
		}
	}
	
	private boolean isEmpty(String str){
		return null==str||"".equals(str.trim());
	}
	
	/**
	 * 判断计数是否大于0
	 * @param conn 数据库连接（本方法不关闭此连接）
	 * @param countSql 计数SQL
	 * @param params 相关参数
	 * @return 是否计数>0
	 * @throws SQLException
	 */
	private boolean exists(Connection conn,String countSql,Object... params) throws SQLException{
		PreparedStatement ps=null;
		ResultSet rs=null;
		try {
			ps = conn.prepareStatement(countSql);
			for(int i=0;i<params.length;i++){
				ps.setObject(i+1, params[i]);
			}
			rs=ps.executeQuery();
			rs.next();
			return rs.getLong(1)>0;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally{
			try {
				if(rs!=null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(ps!=null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public String getSrcTable() {
		return srcTable;
	}
	public void setSrcTable(String srcTable) {
		this.srcTable = srcTable;
	}
	public String getDestTable() {
		return destTable;
	}
	public void setDestTable(String destTable) {
		this.destTable = destTable;
	}
	public String getPkName() {
		return pkName;
	}
	public void setPkName(String pkName) {
		this.pkName = pkName;
	}
	public Connection getSrcConn() {
		return srcConn;
	}
	public void setSrcConn(Connection srcConn) {
		this.srcConn = srcConn;
	}
	public Connection getDestConn() {
		return destConn;
	}
	public void setDestConn(Connection destConn) {
		this.destConn = destConn;
	}
	public String getSelectSql() {
		return selectSql;
	}
	public void setSelectSql(String selectSql) {
		this.selectSql = selectSql;
	}
	/**
	 * 同步表方法
	 * 注：不负责关闭连接
	 */
	public void execute(){
		long startTime=System.currentTimeMillis();
		init();
		StringBuilder insertSql=new StringBuilder();
		StringBuilder updateSql=new StringBuilder();
		PreparedStatement psSrc=null;
		PreparedStatement psInsert=null;
		PreparedStatement psUpdate=null;
		PreparedStatement psDestCount=null;
		PreparedStatement psCountSrc=null;
		try {
			destConn.setAutoCommit(false);
			psSrc=srcConn.prepareStatement(selectSql);
			psCountSrc=srcConn.prepareStatement(countSrcSql);
			ResultSet rsCountSrc=psCountSrc.executeQuery();
			rsCountSrc.next();
			long countSrc=rsCountSrc.getLong(1);
			TableSync.println("共需同步记录数："+countSrc);
			rsCountSrc.close();
			
			ResultSet rsSrc=psSrc.executeQuery();
			insertSql.append(" insert into "+destTable+" ( ");
			updateSql.append(" update "+destTable+" set ");
			List<String> colNames=new ArrayList<String>();
			ResultSetMetaData srcMetaData=rsSrc.getMetaData();
			int cols=srcMetaData.getColumnCount();
			for(int i=0;i<cols;i++){
				String colName=srcMetaData.getColumnName(i+1);
				colNames.add(colName);
				if(i==0){
					insertSql.append(colName);
					updateSql.append(colName+"=?");
				}else{
					insertSql.append(","+colName);
					updateSql.append(","+colName+"=?");
				}
			}
			insertSql.append(") values(");
			updateSql.append(" where "+pkName+"=?");
			
			for(int i=0;i<cols;i++){
				if(i==0){
					insertSql.append("?");
				}else{
					insertSql.append(",?");
				}
			}
			insertSql.append(")");
			TableSync.println(updateSql);
			TableSync.println(insertSql);
			psInsert=destConn.prepareStatement(insertSql.toString());
			psUpdate=destConn.prepareStatement(updateSql.toString());
			
			int countOper=0;//同步记录数
			int countUpdate=0;//更新记录数
			while(rsSrc.next()){
				//判断是否指定关键字段名，若指定则首先查询是否记录已存在，存在则做更新操作
				if(!isEmpty(pkName)){
					Object pk=rsSrc.getObject(pkName);
					if(exists(destConn, countSql,pk)){
						//TODO 待完善
						addUpdate();
						countOper++;
						countUpdate++;
						if(countOper%EXE_BATCH_ROWS==0){
							insertBatchAndLog(psInsert, countSrc, countOper, countUpdate, startTime);
						}
						continue;
					}
				}
				//直接插入
				for(int i=0;i<cols;i++){
					psInsert.setObject(i+1, rsSrc.getObject(i+1));
				}
				psInsert.addBatch();
				countOper++;
				if(countOper%EXE_BATCH_ROWS==0){
					insertBatchAndLog(psInsert, countSrc, countOper, countUpdate, startTime);
				}
			}
			psUpdate.executeBatch();
			insertBatchAndLog(psInsert, countSrc, countOper, countUpdate, startTime);
			rsSrc.close();
			
			psUpdate.close();
			psInsert.close();
			destConn.commit();
			TableSync.println("同步结束，共耗时：["+(System.currentTimeMillis()-startTime)/1000+"]秒");
		} catch (SQLException e) {
			try {
				destConn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally{
			try {
				if(psSrc!=null) psSrc.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(psInsert!=null) psInsert.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(psUpdate!=null) psUpdate.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(psDestCount!=null) psDestCount.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void insertBatchAndLog(PreparedStatement psInsert,long countSrc,int countOper,int countUpdate,long startTime) throws SQLException{
		final String tip="已扫描源表：{0},完成：{1}%,目标表更新：{2}，新增：{3}，耗时：{4}秒";
		psInsert.executeBatch();
		destConn.commit();
		TableSync.println(
			MessageFormat.format(
				tip, countOper,
				(countOper*100.0/(countSrc*1.0)),
				countUpdate,
				countOper-countUpdate,(System.currentTimeMillis()-startTime)/1000)
		);
	}
	
	
	private void assertNull(Object target,String msg){
		if(target==null||target instanceof String && "".equals(target.toString().trim())){
			throw new RuntimeException(msg);
		}
	}
	
	private void addUpdate(){
		//TODO 以下为更新的代码
//		psDestCount=destConn.prepareStatement(countSql);
//		Object pk=rsSrc.getObject(pkName);
//		psDestCount.setObject(1, pk);
//		ResultSet rsDestCount=psDestCount.executeQuery();
//		if(rsDestCount.next()){
//			if(rsDestCount.getInt("COUNTS")>0){
//				rsDestCount.close();
//				psDestCount.close();
//				TableSync.println("continue");
//				continue;
//			}
			//System.out.println("存在记录，跳过。["+pk+"]");
//			for(int i=0;i<cols;i++){
//				psUpdate.setObject(i+1, rsSrc.getObject(i+1));
//			}
//			psUpdate.addBatch();
//			countUpdate++;
//			if(countUpdate%EXE_BATCH_ROWS==0){
//				psUpdate.executeBatch();
//				destConn.commit();
//				TableSync.println("已更新记录数："+countUpdate+",完成百分比："+(countUpdate*100.0/(countSrc*1.0))+"%");
//			}
//		}
//		rsDestCount.close();
//		psDestCount.close();
	}
	
	public static void main(String[] args) {
//			Connection srcConn=DbcpUtil.getConnection("oracle_me_172");
//			Connection desConn=DbcpUtil.getConnection("oracle_me_localhost");
//		copy172_to_172test();
		copy10_to_10test();
//		copy10_to_local();
//		copy172_to_local();
//		copy10_to_172();
//		copyLocal_to_172();
//		copyLocal_to_10();
	}
	/**
	 * 内网复制到到内网测试库
	 */
	private static void copy172_to_172test(){

		try {
			Connection srcConn_172 = DbcpUtil.getConnection("oracle_me_172");
			Connection desConn = DbcpUtil.getConnection("oracle_me_172_test");
			String[] srcSqls=new String[]{
				" select t1.* from fgdj.vv_ent_info t1 right join fgdj.temp t2 on t1.ent_id=t2.ent_id ",
				" select t1.* from fgdj.vv_ent_annl_info t1 right join fgdj.temp t2 on t1.ent_uuid=t2.ent_id ",
				" select t1.* from fgdj.vv_ent_leader t1 right join fgdj.temp t2 on t1.ent_id=t2.ent_id ",
			};
			String[] desTables=new String[]{
				"fgdj.vv_ent_info",
				"fgdj.vv_ent_annl_info",
				"fgdj.vv_ent_leader"
			};
			for(int i=0;i<srcSqls.length;i++){
				System.out.println("开始备份["+srcSqls[i]+"]");
				TableSync task = new TableSync(
					srcSqls[i],
					desTables[i],
					srcConn_172, desConn);
				task.execute();
			}	
			
		} catch (Exception e) {
			DbcpUtil.rollback("oracle_me_172_test");
			e.printStackTrace();
		} finally {
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_172_test");
		}
	
	}
	
	private static void copy10_to_10test(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_10");
			Connection desConn=DbcpUtil.getConnection("oracle_me_10_test");
//			TableSync ins=new TableSync("ETPS.PE_OPERATOR_ENTY", "ETPS.PE_OPERATOR_ENTY", null, srcConn, desConn);
//			TableSync ins=new TableSync("ETPS.ETPS_INFO_ENTY", "ETPS.ETPS_INFO_ENTY", null, srcConn, desConn);
//			TableSync ins=new TableSync("ETPS.ETPS_CONTACT_ENTY", "ETPS.ETPS_CONTACT_ENTY", null, srcConn, desConn);
//			TableSync ins=new TableSync("ETPS.ETPS_SZHY_INFO_ENTY", "ETPS.ETPS_SZHY_INFO_ENTY", null, srcConn, desConn);
//			TableSync ins=new TableSync("notice.NET_USER", "notice.NET_USER", "USER_ID", srcConn, desConn);
//			ins.setSelectSql("select * from etps.etps_info_enty t where area_organ_id like '530102%'");
//			String selectSql="select T1.* from ME.ME_ENTITY_INFO t1 "+
//				" LEFT JOIN (SELECT * FROM ETPS.ETPS_INFO_ENTY "+
//				" where area_organ_id like '530102%')T2 ON T1.ENT_ID=T2.ETPS_ID "+
//				" WHERE T2.ETPS_ID IS NOT NULL ";
//			String selectSql="select x1.* from notice.net_user x1 right join( "+
//				" select * from me.me_entity_info where reg_organ_id like '530102%' "+
//				" )x2 on x1.etps_id=x2.ent_id "+
//				" where x1.etps_id is not null and x2.ent_id is not null ";
//			String selectSql=" select * from ME.ME_ENTITY_INFO ";
			String selectSql="select * from me.me_info";
			TableSync ins=new TableSync();
			ins.setSelectSql(selectSql);
			ins.setPkName("ME_ID");
			ins.setDestTable("ME.ME_INFO");
			ins.setSrcConn(srcConn);
			ins.setDestConn(desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_10_test");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_10_test");
		}
		
	}
	
	private static void copy10_to_local(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_10");
			Connection desConn=DbcpUtil.getConnection("oracle_me_localhost");
			TableSync ins=new TableSync("ETPS.ETPS_MEMBER_ENTY", "ETPS.ETPS_MEMBER_ENTY", null, srcConn, desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_localhost");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_localhost");
		}
		
	}
	
	private static void copyLocal_to_10(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_localhost");
			Connection desConn=DbcpUtil.getConnection("oracle_me_10");
			TableSync ins=new TableSync("FGDJ.SPT_DEPT", "FGDJ.SPT_DEPT", null, srcConn, desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_10");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_localhost");
		}
		
	}
	
	private static void copy10_to_172(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_10");
			Connection desConn=DbcpUtil.getConnection("oracle_me_172");
			TableSync ins=new TableSync("FGDJ.SPT_ORGAN", "FGDJ.SPT_ORGAN_FGDJ", null, srcConn, desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_172");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_10");
			DbcpUtil.release("oracle_me_172");
		}
		
	}
	
	private static void copy172_to_local(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_172");
			Connection desConn=DbcpUtil.getConnection("oracle_me_localhost");
//			TableSync ins=new TableSync("ESF.SPT_ORGAN", "ME_BAK.SPT_ORGAN_GS_BAK", null, srcConn, desConn);
//			TableSync ins=new TableSync("ME.ME_NUMBER_LIMIT", "ME_BAK.ME_NUMBER_LIMIT", null, srcConn, desConn);
			TableSync ins=new TableSync("ME.ME_SUPPORT_INFO", "ME_BAK.ME_SUPPORT_INFO", null, srcConn, desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_localhost");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_localhost");
		}
		
	}
	
	private static void copyLocal_to_172(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_localhost");
			Connection desConn=DbcpUtil.getConnection("oracle_me_172");
			TableSync ins=new TableSync("FGDJ.SPT_ORGAN", "FGDJ.SPT_ORGAN_FGDJ", null, srcConn, desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_172");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_localhost");
		}
		
	}
}
