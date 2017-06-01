package db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import utils.DbcpUtil;

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
		int idx=selectSql.toUpperCase().indexOf("FROM");
		countSrcSql="select count(*) AS COUNTS "+selectSql.substring(idx,selectSql.length());
		System.out.println(countSrcSql);
		if(!isEmpty(pkName)){
			countSql="select count(*) AS COUNTS from "+destTable+" where "+pkName+"=?";
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
	
	/**
	 * 同步表方法
	 * 注：不负责关闭连接
	 * @throws SQLException 
	 */
	public void execute() throws SQLException{
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
						addUpdate();//TODO 待完善
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
			throw e;
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
//		copy172_to_172test();
//		copy10_to_10test();
//		copy10_to_local();
//		copy172_to_local();
//		copy10_to_172();
//		copyLocal_to_172();
//		copyLocal_to_10();
//		dagl_informix2oracle();
		dagl_informix61_2_oracle();
//		dagl_informix61_framework_2_oracle();
	}
	
	
	public static void dagl_informix61_framework_2_oracle(){

		try {
			Connection srcConn = DbcpUtil.getConnection("informix_61");
			Connection desConn = DbcpUtil.getConnection("oracle_dagl");
			String[] srcSqls=new String[]{
//				"SELECT * FROM FRAMEWORK:BPROXY_ACT_PERM",//DONE
//				"SELECT * FROM FRAMEWORK:MENU_RESOURCE",//DONE
//				"SELECT * FROM FRAMEWORK:NODE_USER",//DONE
//				"SELECT * FROM FRAMEWORK:ORGAN_NODE where name is not null and name!='' ",//DONE
//				"SELECT * FROM FRAMEWORK:PROXY_PERMISSION",//DONE
//				"SELECT * FROM FRAMEWORK:PROXY_USER",//DONE
//				"SELECT * FROM FRAMEWORK:SC_ACC_PERMISSION",//DONE
//				"SELECT * FROM FRAMEWORK:SC_ACL_PERMISSION",//DONE
//				"SELECT * FROM FRAMEWORK:SC_ACL_RESOURCE where NATIVE_RESOURCE_ID is not null and NATIVE_RESOURCE_ID!=''",
//				"SELECT * FROM FRAMEWORK:SC_GROUP",//DONE
//				"SELECT * FROM FRAMEWORK:SC_GROUP_ROLE",//DONE
//				"SELECT * FROM FRAMEWORK:SC_USER",//DONE
//				"SELECT * FROM FRAMEWORK:SC_USER_GROUP",//DONE
//				"SELECT * FROM FRAMEWORK:SC_USER_ROLE",//DONE
//				"SELECT * FROM FRAMEWORK:SSO_PASS"//DONE
			};
			String[] desTables=new String[]{
			};
			
			for(int i=0;i<srcSqls.length;i++){
				TableSync.println("------------------------------------------------------------------");
				System.out.println("开始备份["+srcSqls[i]+"]");
				TableSync task = new TableSync(
					srcSqls[i],
					desTables[i],
					srcConn, desConn);
				task.execute();
			}	
			
		} catch (Exception e) {
			DbcpUtil.rollback("oracle_dagl");
			e.printStackTrace();
		} finally {
			DbcpUtil.release("informix_61");
			DbcpUtil.release("oracle_dagl");
		}
	
	}
	
	public static void dagl_informix61_2_oracle(){

		try {
			Connection srcConn = DbcpUtil.getConnection("informix_61");
			Connection desConn = DbcpUtil.getConnection("oracle_dagl");
			String[] srcSqls=new String[]{
//				"select * from dic:DIC_INDUSTRY_GB",//DONE
//				"select * from framework:DIC_LOAD_CONFIG",//DONE
//				"select etps_id,is_union_apart,etps_name,reg_no,zmq_zone from ETPS:ETPS_OTHER_INFO_ENTY",//DONE,减少复制字段，数据太多，复制少量
				"select * from dic:dic_obj_app_type"//DONE
			};
			String[] desTables=new String[]{
				"etps.dic_obj_app_type"
			};
			
			for(int i=0;i<srcSqls.length;i++){
				TableSync.println("------------------------------------------------------------------");
				System.out.println("开始备份["+srcSqls[i]+"]");
				TableSync task = new TableSync(
					srcSqls[i],
					desTables[i],
					srcConn, desConn);
				task.execute();
			}	
			
		} catch (Exception e) {
			DbcpUtil.rollback("oracle_dagl");
			e.printStackTrace();
		} finally {
			DbcpUtil.release("informix_61");
			DbcpUtil.release("oracle_dagl");
		}
	
	}
	
	public static void dagl_informix2oracle(){
		
		try {
			Connection srcConn = DbcpUtil.getConnection("informix_62");
			Connection desConn = DbcpUtil.getConnection("oracle_dagl");
			String[] srcSqls=new String[]{
//				" select * from archives:app_version_detail ",//DONE
//				" select * from archives:app_version_info ",//DONE
//				" select * from archives:ARCHV_APP "//DONE
//				"select app_id,doc_no,doc_year,title,ori_date,charge_person,file_no,dic_result_id,dossier,dic_secret_degree,unit,box,handover_person,file_linkpath,doc_type,noa_app_id,case_app_id,case_date,number as number_,case_organ_id,reg_no from archives:ARCHV_APP_DOC",////DONE
//				"select * from archives:ARCHV_APP_ETPS",//DONE
//				"select * from archives:ARCHV_APP_NOA",//DONE,添加字段解决
//				"select * from archives:ARCHV_APP_NOA_FILE",//DONE
//				"select * from archives:ARCHV_BORROW",//DONE
//				"select * from archives:ARCHV_BORROW_STUFF",//DONE
//				"select * from archives:ARCHV_BULLETIN",//DONE
//				"select * from archives:ARCHV_CACHE",//DONE
//				"select * from archives:ARCHV_CACHE_FILE",//DONE
//				"select * from archives:ARCHV_CATALOG",//DONE
//				"select * from archives:ARCHV_CD_INFO",//DONE
//				"select * from archives:ARCHV_CONFIG",//DONE
//				"select archv_id,doc_no,doc_year,title,ori_date,charge_person,file_no,dic_result_id,dossier,dic_secret_degree,unit,box,handover_person,file_linkpath,doc_type,archv_organ_id,archv_no,old_archv_no,app_id,dept_name,dept_id,archv_status,case_date,number as number_,case_organ_id,reg_no from archives:ARCHV_DOC",
//				"select * from archives:ARCHV_DOC_TMP",//TODO 未找到数据表，暂不管
//				"select * from archives:ARCHV_EDOC_ETPS_ATV",//DONE
//				"select * from archives:ARCHV_EDOC_ETPS_ENT",//DONE
//				"select * from archives:ARCHV_EDOC_FOLDER_ATV",//DONE
//				"select * from archives:ARCHV_EDOC_FOLDER_ENT",//DONE
//				"select * from archives:ARCHV_EDOC_FOLDER_ENT_OLD",//暂不要
//				"select * from archives:ARCHV_EDOC_FOLDER_ENT_OLD1",//暂不要
//				"select * from archives:ARCHV_EDOC_MOVE_LIST",//DONE
//				"select * from archives:ARCHV_EDOC_OPNN",//DONE
//				"select * from archives:ARCHV_EDOC_PAGE_ATV",//DONE
//				"select * from archives:ARCHV_EDOC_PAGE_ENT",//DONE
//				"select * from archives:ARCHV_ERROR_ENTITY",//DONE
//				"select * from archives:ARCHV_ERROR_INFO",//DONE
//				"select * from archives:ARCHV_ETPS",//DONE
//				"select * from archives:ARCHV_ETPS_BAK",//暂不要
//				"select * from archives:ARCHV_INFO",//DONE 添加字段
//				"select * from archives:ARCHV_INFO_TMP",//暂不要
//				"select * from archives:ARCHV_JUDGE",//DONE
//				"select * from archives:ARCHV_JUDGE_LIST",//DONE
//				"select * from archives:ARCHV_NOA",//DONE 添加字段
//				"select * from archives:ARCHV_NOA_FILE",//DONE
//				"select * from archives:ARCHV_OPNN",//DONE
//				"select * from archives:ARCHV_PUB_USER",
//				"select * from archives:ARCHV_REVIEW_LIST",//DONE
//				"select * from archives:ARCHV_SCAN_FOLDER",//DONE
//				"select * from archives:ARCHV_SCAN_SUB",//DONE
//				"select * from archives:ARCHV_STORE_ROOM",//DONE
//				"select * from archives:ARCHV_SYS_MAX_SERIAL",
//				"select * from archives:ARCHV_SYS_NUMBS",//DONE
//				"select * from archives:ARCHV_TRANS_TASK",//DONE
//				"select * from archives:ARCHV_WEBSVR_OPNN",//DONE,取消空值限制
//				"select * from archives:ARCH_QRY_LOG",//DONE
//				"select * from archives:ARCH_QRY_PE_LOG",//DONE
//				"select * from archives:CASE_ARCHV_APP",//DONE
//				"select id,app_id,order_id,file_no,duty_user_id,duty_user_name,title,create_date,page_no,dic_sercret_degree,key_word,class_no,appendix,memo,case_date,number as number_,case_organ_id,reg_no from archives:CASE_ARCHV_APP_FILE",
//				"select * from archives:DEAL_STEP",//TODO 找不到相应表
//				"select * from archives:DIC_AUTH",//DONE
//				"select * from archives:DIC_CHANGE",//DONE
//				"select * from archives:DIC_DOSSIER",//DONE
//				"select * from archives:DIC_FLOW",//DONE
//				"select * from archives:DIC_FLOW_DATA",//TODO 复制数据失败，TEXT-->CLOB
//				"select * from archives:DIC_FOLDER",//DONE
//					"select * from BUSINESS:DIC_INDUSTRY_GB",
//					"select * from archives:DIC_LOAD_CONFIG",
//					"select  id,mode as mode_,page_no,sub_obj_id,secret_degree,name,page_nums,efficacy,archv_organ_id from archives:DIC_PAGE",
//				"select * from archives:DIC_RECORD",//DONE
//				"select * from archives:DIC_SECRET_DEGREE",//DONE
//				"select * from archives:DIC_STOCK_MODE",//DONE
//				"select * from archives:DIC_STORE_ROOM",//DONE
//				"select * from archives:DIC_SUPERVISE",//DONE
//				"select * from gzsq:DIC_SM",//DONE
//				"select * from archives:ETPS_ARCHV_APP",//DONE
//					"select * from archives:ETPS_ARCHV_APP_OPNN WHERE APP_ID IS NOT NULL AND APP_ID!='' ",//DONE
//					"select * from archives:ETPS_OTHER_INFO_ENTY",//61,ETPS
//					"select * from archives:FIX_PAGE_ANNL_MJ",//不处理
//					"select * from archives:FL_ACTIVITY",//不处理
//				"select * from archives:FL_ACTIVITY_AUTO",//DONE
//				"select * from archives:FL_ACTIVITY_DAYS",//DONE
//				"select * from archives:FL_ACTIVITY_PARAMETER",//DONE
//				"select * from archives:FL_ACTIVITY_RELATION",//DONE
//				"select * from archives:FL_ACTIVITY_USER",//DONE
//				"select * from archives:FL_ACTIVITY_VIEW",//DONE
//				"select * from archives:FL_INST",//DONE
//				"select * from archives:FL_INST_CHANGE",//DONE
//				"select * from archives:FL_PARAMETER",//DONE
//					"select * from archives:FRAG_LOGS",//不处理
//				"select * from archives:MOVE_ETPS_DATA_TMP",//DONE
//				"select * from archives:NOA_ARCHV_APP",//DONE
//					"select * from archives:NOA_ARCHV_APP_FILE",//不处理
//				"select * from archives:QUERY_EXCLUDE_LIST",//DONE
//					"select * from archives:TMP_LZ"//不处理
			};
			String[] desTables=new String[]{
//					"FIX_PAGE_ANNL_MJ",
//					"FL_ACTIVITY",
//					"FRAG_LOGS",
//					"NOA_ARCHV_APP_FILE",
//					"TMP_LZ"
			};
			
			for(int i=0;i<srcSqls.length;i++){
				TableSync.println("------------------------------------------------------------------");
				System.out.println("开始备份["+srcSqls[i]+"]");
				TableSync task = new TableSync(
						srcSqls[i],
						desTables[i],
						srcConn, desConn);
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
			TableSync ins=new TableSync("notice.NET_USER", "notice.NET_USER", "USER_ID", srcConn, desConn);
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
	
	private static void copy172_to_10(){
		
		try {
			Connection srcConn=DbcpUtil.getConnection("oracle_me_172");
			Connection desConn=DbcpUtil.getConnection("oracle_me_10");
			TableSync ins=new TableSync("ME.me_entity_info", "ME.me_entity_info", null, srcConn, desConn);
			ins.execute();
		} catch (SQLException e) {
			DbcpUtil.rollback("oracle_me_10");
			e.printStackTrace();
		} finally{
			DbcpUtil.release("oracle_me_172");
			DbcpUtil.release("oracle_me_10");
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
