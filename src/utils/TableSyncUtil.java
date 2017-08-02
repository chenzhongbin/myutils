package utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * 表同步工具
 * @ClassName: TableSycUtil 
 * @author chenzhongbin
 * @date 2017-6-22 下午6:04:00 
 * @version V1.0 
 * @since V1.0
 */
public class TableSyncUtil {
	
	public enum SyncType{
		INSERT_ALL,	//全部插入目标表
		INSERT_NEW,
		UPDATE,	//新增数据忽略，已有数据更新
		INSERT_UPDATE//新增数据插入，已有数据更新
	}
	
	public static class Config{
		private String uuid;
		private Connection srcConn;
		private Connection destConn;
		private String srcDsName;
		private String destDsName;
		private SyncType syncType;//同步类型
		private String srcTable;//来源表
		private String srcSql;//来源查询语句，优先级<来源表
		private String destTable;//目标表
		private String[] pkNames;//主键名，支持联合主键
		
		private String updateWhereSql;
		private String[] updateWhereCols=new String[]{};
		
		private int srcTotal=0;
		private int destUpdate=0;
		private int destInsert=0;
		private int scanCount=0;
		private final int BATCH_NUM=1000;
		private long startTime;
		
		public String getUuid() {
			return uuid;
		}
		public void setUuid(String uuid) {
			this.uuid = uuid;
		}
		public String getUpdateWhereSql() {
			return updateWhereSql;
		}
		public void setUpdateWhereSql(String updateWhereSql) {
			this.updateWhereSql = updateWhereSql;
		}
		public String[] getUpdateWhereCols() {
			return updateWhereCols;
		}
		public void setUpdateWhereCols(String[] updateWhereCols) {
			this.updateWhereCols = updateWhereCols;
		}
		public void setUpdateWhereCols(String updateWhereCols) {
			if(updateWhereCols==null||"".equals(updateWhereCols.trim())){
				this.updateWhereCols=new String[]{};
			}else{
				this.updateWhereCols = updateWhereCols.trim().split(",");
			}
		}
		
		public String getSrcDsName() {
			return srcDsName;
		}
		public void setSrcDsName(String srcDsName) {
			this.srcDsName = srcDsName;
		}
		public String getDestDsName() {
			return destDsName;
		}
		public void setDestDsName(String destDsName) {
			this.destDsName = destDsName;
		}
		public Connection getSrcConn() {
			return srcConn;
		}
		public Connection getDestConn() {
			return destConn;
		}
		public String getSrcTable() {
			return srcTable;
		}
		public void setSrcTable(String srcTable) {
			this.srcTable = srcTable;
		}
		public String getSrcSql() {
			return srcSql;
		}
		public void setSrcSql(String srcSql) {
			this.srcSql = srcSql;
		}
		public String getDestTable() {
			return destTable;
		}
		public void setDestTable(String destTable) {
			this.destTable = destTable;
		}
		public String[] getPkNames() {
			return pkNames;
		}
		public void setPkNames(String pkNames) {
			if(!StringUtils.isEmpty(pkNames)){
				this.pkNames = pkNames.trim().split(",");
			}
		}
		public void setPkNames(String[] pkNames) {
			this.pkNames = pkNames;
		}
		public SyncType getSyncType() {
			return syncType;
		}
		public void setSyncType(SyncType syncType) {
			this.syncType = syncType;
		}
		
	}
	
	private Config config;
	
	public void initConfig(Config config){
		this.config=config;
		this.config.srcConn=DbcpUtil.getConnection(this.config.srcDsName);
		this.config.destConn=DbcpUtil.getConnection(this.config.destDsName);
	}
	
	private void checkConfig(){
		assertNull(this.config,"未提供配置！");
		assertNull(this.config.srcConn,"未提供同步来源数据库连接！");
		assertNull(this.config.destConn,"未提供同步目标数据库连接！");
		assertNull(this.config.destTable,"未指定同步目标表！");
		assertNull(this.config.syncType,"未指定同步类型！");
		if(isEmpty(this.config.srcTable) && isEmpty(this.config.srcSql)){
			throw new RuntimeException("未指定来源数据表或来源数据查询SQL！");
		}
		if(this.config.syncType==SyncType.UPDATE||this.config.syncType==SyncType.INSERT_UPDATE){
			assertNull(this.config.pkNames, "未指定主键！");
		}
	}
	
	private void assertNull(Object target,String msg){
		if(isEmpty(target)){
			throw new RuntimeException(msg);
		}
	}
	
	private boolean isEmpty(Object target){
		if(target==null||target instanceof String && "".equals(target.toString().trim())){
			return true;
		}
		return false;
	}
	
	private List<String> _getColNames(ResultSetMetaData srcMetaData) throws SQLException{
		List<String> ret=new ArrayList<String>();
		int cols=srcMetaData.getColumnCount();
		for(int i=0;i<cols;i++){
			String colName=srcMetaData.getColumnName(i+1);
			ret.add(colName);
		}
		return ret;
	}
	
	private String _buildInsertSql(List<String> colNames){
		String destTable=config.destTable;
		StringBuilder sql=new StringBuilder(" insert into "+destTable+" ( ");
		boolean moreThanOneCol=false;
		for(String name:colNames){
			if(moreThanOneCol){
				sql.append(","+name);
			}else{
				sql.append(name);
				moreThanOneCol=true;
			}
		}
		sql.append(") values (");
		moreThanOneCol=false;
		for(int i=0;i<colNames.size();i++){
			if(moreThanOneCol){
				sql.append(",?");
			}else{
				sql.append("?");
				moreThanOneCol=true;
			}
		}
		sql.append(") ");
		return sql.toString();
	}
	
	private String _buildDestCountSql(){
		String destTable=config.destTable;
		String[] pkNames=config.pkNames;
		StringBuilder sql=new StringBuilder(" select count(*) from "+destTable+" where 1=1 ");
		for(int i=0;i<pkNames.length;i++){
			sql.append(" and "+pkNames[i]+"=? ");
		}
		return sql.toString();
	}
	
	private String _buildDestUpdateCountSql(){
		String destTable=config.destTable;
		String[] pkNames=config.pkNames;
		StringBuilder sql=new StringBuilder(" select count(*) from "+destTable+" where 1=1 ");
		for(int i=0;i<pkNames.length;i++){
			sql.append(" and "+pkNames[i]+"=? ");
		}
		if(!isEmpty(config.updateWhereSql)){
			sql.append(" and ("+config.updateWhereSql+")");
		}
		return sql.toString();
	}
	
	private String _buildUpdateSql(List<String> colNames){
		String destTable=config.destTable;
		String[] pkNames=config.pkNames;
		if(pkNames==null||pkNames.length<1){
			//如果没有提供主键，生成的更新语句将更新全表数据，很危险！
			throw new RuntimeException("主键为空！无法生成更新SQL！");
		}
		StringBuilder sql=new StringBuilder(" update "+destTable+" set ");
		boolean moreThanOne=false;
		for(String name:colNames){
			if(moreThanOne){
				sql.append(","+name+"=? ");
			}else{
				sql.append(name+"=? ");
				moreThanOne=true;
			}
		}
		sql.append(" where 1=1 ");
		for(String pkName:pkNames){
			sql.append(" and "+pkName+"=? ");
		}
		if(config.updateWhereCols!=null && !isEmpty(config.updateWhereSql)){
			if(config.updateWhereSql.trim().toLowerCase().startsWith("and")){
				sql.append(config.updateWhereSql);
			}else{
				sql.append(" and "+config.updateWhereSql);
			}
		}
		System.out.println(sql.toString());
		return sql.toString();
	}
	
	private String _buildCountSql(String selectSql){
		return "select count(*) from ("+selectSql+")";
	}
	
	private void insertRow(List<String> colNames,ResultSet rsSrc,PreparedStatement psInsert) throws SQLException{
		for(int i=0;i<colNames.size();i++){
			psInsert.setObject(i+1, rsSrc.getObject(colNames.get(i)));
		}
		psInsert.addBatch();
		config.destInsert++;
		config.scanCount++;
	}
	
	private void insertNewRow(List<String> colNames,ResultSet rsSrc,PreparedStatement psInsert,PreparedStatement psDestCount) throws SQLException{
		int destCount=_getDestCount(rsSrc,psDestCount);
		config.scanCount++;
		if(destCount>0) 
			return;
		
		for(int i=0;i<colNames.size();i++){
			psInsert.setObject(i+1, rsSrc.getObject(colNames.get(i)));
		}
		psInsert.addBatch();
		config.destInsert++;
	}
	
	private void updateRow(List<String> colNames,ResultSet rsSrc,PreparedStatement psUpdate,PreparedStatement psDestCount,PreparedStatement psDestUpdateCount) throws SQLException{
		int destCount=_getDestCount(rsSrc,psDestCount);
		int destUpdateCount=_getDestUpdateCount(rsSrc,psDestUpdateCount);
		
		if(destCount>1){
			throw new RuntimeException("目标表记录重复！");
		}
		config.scanCount++;
		if(destCount<1 || destUpdateCount<1) return;
		config.destUpdate++;
		
		for(int i=0;i<colNames.size();i++){
			psUpdate.setObject(i+1, rsSrc.getObject(colNames.get(i)));
		}
		String[] pkNames=config.pkNames;
		for(int i=0;i<pkNames.length;i++){
			psUpdate.setObject(colNames.size()+i+1, rsSrc.getObject(pkNames[i]));
		}
		if(config.updateWhereCols!=null && !isEmpty(config.updateWhereSql)){
			for(int i=0;i<config.updateWhereCols.length;i++){
				psUpdate.setObject(colNames.size()+config.pkNames.length+i+1,rsSrc.getObject(config.updateWhereCols[i]));
			}
		}
		psUpdate.addBatch();
	}
	
	private int _getDestCount(ResultSet rsSrc,PreparedStatement psDestCount) throws SQLException{
		String[] pkNames=config.pkNames;
		for(int i=0;i<pkNames.length;i++){
			psDestCount.setObject(i+1, rsSrc.getObject(pkNames[i]));
		}
		ResultSet rsDestCount=psDestCount.executeQuery();
		try {
			rsDestCount.next();
			return rsDestCount.getInt(1);
		} catch (SQLException e) {
			throw e;
		} finally{
			rsDestCount.close();
		}
	}
	
	private int _getDestUpdateCount(ResultSet rsSrc,PreparedStatement psDestUpdateCount) throws SQLException{
		String[] pkNames=config.pkNames;
		for(int i=0;i<pkNames.length;i++){
			psDestUpdateCount.setObject(i+1, rsSrc.getObject(pkNames[i]));
		}
		if(config.updateWhereCols!=null && !isEmpty(config.updateWhereSql)){
			for(int i=0;i<config.updateWhereCols.length;i++){
				psDestUpdateCount.setObject(config.pkNames.length+i+1,rsSrc.getObject(config.updateWhereCols[i]));
			}
		}
		ResultSet rsDestCount=psDestUpdateCount.executeQuery();
		try {
			rsDestCount.next();
			return rsDestCount.getInt(1);
		} catch (SQLException e) {
			throw e;
		} finally{
			rsDestCount.close();
		}
	}
	
	private void insertOrUpdateRow(List<String> colNames,ResultSet rsSrc,PreparedStatement psInsert,PreparedStatement psUpdate,PreparedStatement psDestCount,PreparedStatement psDestUpdateCount) throws SQLException{
		int destCount=_getDestCount(rsSrc,psDestCount);
		//目标表已有数据
		if(destCount>0){
			updateRow(colNames, rsSrc, psUpdate,psDestCount,psDestUpdateCount);//更新
		}else{
			insertRow(colNames, rsSrc, psInsert);//插入
		}
	}
	
	private void executeBatch(PreparedStatement psInsert,PreparedStatement psUpdate,boolean commitForcely) throws SQLException{
		if(config.scanCount%config.BATCH_NUM==0 || commitForcely ){
			final String tip="已扫描源表：{0},完成：{1} %,目标表更新：{2}，新增：{3}，耗时：{4}秒";
			if(psInsert!=null){
				psInsert.executeBatch();
			}
			if(psUpdate!=null){
				psUpdate.executeBatch();
			}
			config.destConn.commit();
			System.out.println(
				MessageFormat.format(
						tip, config.scanCount,
						(config.scanCount*100.0/(config.srcTotal*1.0)),
						config.destUpdate,
						config.destInsert,(System.currentTimeMillis()-config.startTime)/1000)
				);
		}
	}
	
	private static void setConfigValid(String dsName,String configTable,String uuid,String valid) throws Exception{
		Connection conn=null;
		try {
			conn=DbcpUtil.getConnection(dsName);
			conn.setAutoCommit(false);
			String sql="update "+configTable+" set valid=? where uuid=?";
			PreparedStatement ps=conn.prepareStatement(sql);
			ps.setString(1, valid);
			ps.setString(2, uuid);
			ps.executeUpdate();
			ps.close();
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		} finally{
			DbcpUtil.close(conn);
		}
	}
	
	public static void executeSyncBatch(String dsName,String configTable) throws Exception{
		List<Config> configs=readConfigs(dsName,configTable);
		for(Config config:configs){
			TableSyncUtil util=new TableSyncUtil();
			util.initConfig(config);
			util.executeSync();
			setConfigValid(dsName,configTable,config.getUuid(),"0");
		}
	}
	
	private static List<Config> readConfigs(String dsName,String configTable) throws SQLException{
		try {
			List<Config> configs=new ArrayList<Config>();
			Connection conn=DbcpUtil.getConnection(dsName);
			PreparedStatement ps=conn.prepareStatement("select * from "+configTable+" where valid='1'");
			ResultSet rs=ps.executeQuery();
			while(rs.next()){
				Config config=new Config();
				config.setUuid(rs.getString("UUID"));
				config.setSrcDsName(rs.getString("SRC_DS_NAME"));
				config.setDestDsName(rs.getString("DEST_DS_NAME"));
				config.setSrcTable(rs.getString("SRC_TABLE"));
				config.setSrcSql(rs.getString("SRC_SQL"));
				config.setSrcTable(rs.getString("SRC_TABLE"));
				config.setDestTable(rs.getString("DEST_TABLE"));
				config.setPkNames(rs.getString("PK_NAMES"));
				config.setUpdateWhereCols(rs.getString("UPDATE_WHERE_COLS"));
				config.setUpdateWhereSql(rs.getString("UPDATE_WHERE_SQL"));
				String syncType=rs.getString("SYNC_TYPE");
				if("INSERT_UPDATE".equals(syncType)){
					config.setSyncType(SyncType.INSERT_UPDATE);
				}else if("INSERT_ALL".equals(syncType)){
					config.setSyncType(SyncType.INSERT_ALL);
				}else if("INSERT_NEW".equals(syncType)){
					config.setSyncType(SyncType.INSERT_NEW);
				}else if("UPDATE".equals(syncType)){
					config.setSyncType(SyncType.UPDATE);
				}
				configs.add(config);
			}
			return configs;
		} catch (SQLException e) {
			throw e;
		} finally{
			DbcpUtil.release(dsName);
		}
	}
	
	public void executeSync() throws SQLException{

		checkConfig();//同步前检查配置
		
		config.startTime=System.currentTimeMillis();
		String destInsertSql=null;
		String destUpdateSql=null;
		String destCountSql=null;
		String destUpdateCountSql=null;
		
		PreparedStatement psSrc=null;
		PreparedStatement psInsert=null;
		PreparedStatement psUpdate=null;
		PreparedStatement psDestCount=null;
		PreparedStatement psDestUpdateCount=null;
		PreparedStatement psCountSrc=null;
		List<String> colNames=null;
		try {
			config.destConn.setAutoCommit(false);
			if(!isEmpty(config.srcTable)){
				config.srcSql="select * from "+config.srcTable;
			}
			psSrc=config.srcConn.prepareStatement(config.srcSql);
			System.out.println(config.srcSql);
			psCountSrc=config.srcConn.prepareStatement(_buildCountSql(config.srcSql));
			ResultSet rsCountSrc=psCountSrc.executeQuery();
			rsCountSrc.next();
			config.srcTotal=rsCountSrc.getInt(1);
			System.out.println("共需同步记录数："+config.srcTotal);
			rsCountSrc.close();
			
			ResultSet rsSrc=psSrc.executeQuery();
			colNames=_getColNames(rsSrc.getMetaData());
			destInsertSql=_buildInsertSql(colNames);
			psInsert=config.destConn.prepareStatement(destInsertSql);
			
			switch(this.config.syncType){
			//源表全部复制到目标表
			case INSERT_ALL:
				while(rsSrc.next()){
					insertRow(colNames, rsSrc, psInsert);
					executeBatch(psInsert, psUpdate,false);
				}
				executeBatch(psInsert, psUpdate,true);
				break;
			//源表新记录复制到目标表
			case INSERT_NEW:
				destCountSql=_buildDestCountSql();
				psDestCount=config.destConn.prepareStatement(destCountSql);
				while(rsSrc.next()){
					insertNewRow(colNames, rsSrc, psInsert,psDestCount);
					executeBatch(psInsert, psUpdate,false);
				}
				executeBatch(psInsert, psUpdate,true);
				break;
				
			case UPDATE:
				destUpdateSql=_buildUpdateSql(colNames);
				destCountSql=_buildDestCountSql();
				destUpdateCountSql=_buildDestUpdateCountSql();
				psUpdate=config.destConn.prepareStatement(destUpdateSql);
				psDestCount=config.destConn.prepareStatement(destCountSql);
				psDestUpdateCount=config.destConn.prepareStatement(destUpdateCountSql);
				while(rsSrc.next()){
					updateRow(colNames, rsSrc, psUpdate,psDestCount,psDestUpdateCount);
					executeBatch(psInsert, psUpdate,false);
				}
				executeBatch(psInsert, psUpdate,true);
				break;
				
			case INSERT_UPDATE:
				destUpdateSql=_buildUpdateSql(colNames);
				destCountSql=_buildDestCountSql();
				destUpdateCountSql=_buildDestUpdateCountSql();
				psUpdate=config.destConn.prepareStatement(destUpdateSql);
				psDestCount=config.destConn.prepareStatement(destCountSql);
				psDestUpdateCount=config.destConn.prepareStatement(destUpdateCountSql);
				while(rsSrc.next()){
					insertOrUpdateRow(colNames,rsSrc,psInsert,psUpdate,psDestCount,psDestUpdateCount);
					executeBatch(psInsert, psUpdate,false);
				}
				executeBatch(psInsert, psUpdate,true);
				break;
				
			default:
				//do nothing
				break;
			}
			config.destConn.commit();
			System.out.println("同步结束，共耗时：["+(System.currentTimeMillis()-config.startTime)/1000.0+"]秒");
		} catch (SQLException e) {
			try {
				config.destConn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			throw e;
		} finally{
			DbcpUtil.close(psSrc,psInsert,psUpdate,psDestCount,psDestUpdateCount);
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("准备开始同步...");
		TableSyncUtil.executeSyncBatch("config","table_sync_config");
		System.out.println("同步结束！");
	}
	
}

