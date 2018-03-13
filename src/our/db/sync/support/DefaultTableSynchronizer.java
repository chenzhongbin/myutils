package our.db.sync.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

public class DefaultTableSynchronizer extends AbstractSynchronizer {
	
	private DataSource configDataSource;
	
	private String configTableName;

	private Map<String,DataSource> dataSoureMap;
	
	private List<SyncConfig> syncConfigList;
	
	@Override
	public void synchronize() throws SQLException {
		//加载需要同步的信息
		loadSyncConfigList();
		//执行同步
		executeSync();
	}
	
	private void loadSyncConfigList() throws SQLException {
		
		this.syncConfigList=new ArrayList<SyncConfig>();
		
		if(this.configTableName==null) {
			this.configTableName="SYNC_CONFIG";
		}
		
		Connection conn=null;
		try {
			conn=configDataSource.getConnection();
			PreparedStatement ps=conn.prepareStatement("select * from "+this.configTableName+" where valid='1'");
			ResultSet rs=ps.executeQuery();
			while(rs.next()){
				SyncConfig config=new SyncConfig();
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
				syncConfigList.add(config);
			}
		} finally{
			if(conn!=null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private void checkConfig(SyncConfig config){
		assertNull(config,"未提供配置！");
		assertNull(config.getSrcConn(),"未提供同步来源数据库连接！");
		assertNull(config.getDestConn(),"未提供同步目标数据库连接！");
		assertNull(config.getDestTable(),"未指定同步目标表！");
		assertNull(config.getSyncType(),"未指定同步类型！");
		if(isEmpty(config.getSrcTable()) && isEmpty(config.getSrcSql())){
			throw new RuntimeException("未指定来源数据表或来源数据查询SQL！");
		}
		if(config.getSyncType()==SyncType.UPDATE||config.getSyncType()==SyncType.INSERT_UPDATE){
			assertNull(config.getPkNames(), "未指定主键！");
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
	
	private List<String> getColNames(ResultSetMetaData srcMetaData) throws SQLException{
		List<String> ret=new ArrayList<String>();
		int cols=srcMetaData.getColumnCount();
		for(int i=0;i<cols;i++){
			String colName=srcMetaData.getColumnName(i+1);
			ret.add(colName);
		}
		return ret;
	}
	
	private String buildInsertSql(SyncConfig config,List<String> colNames){
		String destTable=config.getDestTable();
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
	
	private String buildDestCountSql(SyncConfig config){
		String destTable=config.getDestTable();
		String[] pkNames=config.getPkNames();
		StringBuilder sql=new StringBuilder(" select count(*) from "+destTable+" where 1=1 ");
		for(int i=0;i<pkNames.length;i++){
			sql.append(" and "+pkNames[i]+"=? ");
		}
		return sql.toString();
	}
	
	private String buildDestUpdateCountSql(SyncConfig config){
		String destTable=config.getDestTable();
		String[] pkNames=config.getPkNames();
		StringBuilder sql=new StringBuilder(" select count(*) from "+destTable+" where 1=1 ");
		for(int i=0;i<pkNames.length;i++){
			sql.append(" and "+pkNames[i]+"=? ");
		}
		if(!isEmpty(config.getUpdateWhereSql())){
			sql.append(" and ("+config.getUpdateWhereSql()+")");
		}
		return sql.toString();
	}
	
	private String buildUpdateSql(SyncConfig config,List<String> colNames){
		String destTable=config.getDestTable();
		String[] pkNames=config.getPkNames();
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
		if(config.getUpdateWhereCols()!=null && !isEmpty(config.getUpdateWhereSql())){
			if(config.getUpdateWhereSql().trim().toLowerCase().startsWith("and")){
				sql.append(config.getUpdateWhereSql());
			}else{
				sql.append(" and "+config.getUpdateWhereSql());
			}
		}
		System.out.println(sql.toString());
		return sql.toString();
	}
	
	private String buildCountSql(String selectSql){
		return "select count(*) from ("+selectSql+") t";
	}
	
	private void insertRow(SyncConfig config,List<String> colNames,ResultSet rsSrc,PreparedStatement psInsert) throws SQLException{
		for(int i=0;i<colNames.size();i++){
			psInsert.setObject(i+1, rsSrc.getObject(colNames.get(i)));
		}
		psInsert.addBatch();
		config.destInsert++;
		config.scanCount++;
	}
	
	private void insertNewRow(SyncConfig config,List<String> colNames,ResultSet rsSrc,PreparedStatement psInsert,PreparedStatement psDestCount) throws SQLException{
		int destCount=getDestCount(config,rsSrc,psDestCount);
		config.scanCount++;
		if(destCount>0) 
			return;
		
		for(int i=0;i<colNames.size();i++){
			psInsert.setObject(i+1, rsSrc.getObject(colNames.get(i)));
		}
		psInsert.addBatch();
		config.destInsert++;
	}
	
	private void updateRow(SyncConfig config,List<String> colNames,ResultSet rsSrc,PreparedStatement psUpdate,PreparedStatement psDestCount,PreparedStatement psDestUpdateCount) throws SQLException{
		int destCount=getDestCount(config,rsSrc,psDestCount);
		int destUpdateCount=getDestUpdateCount(config,rsSrc,psDestUpdateCount);
		
		if(destCount>1){
			throw new RuntimeException("目标表记录重复！");
		}
		config.scanCount++;
		if(destCount<1 || destUpdateCount<1) return;
		config.destUpdate++;
		
		for(int i=0;i<colNames.size();i++){
			psUpdate.setObject(i+1, rsSrc.getObject(colNames.get(i)));
		}
		String[] pkNames=config.getPkNames();
		for(int i=0;i<pkNames.length;i++){
			psUpdate.setObject(colNames.size()+i+1, rsSrc.getObject(pkNames[i]));
		}
		if(config.getUpdateWhereCols()!=null && !isEmpty(config.getUpdateWhereSql())){
			for(int i=0;i<config.getUpdateWhereCols().length;i++){
				psUpdate.setObject(colNames.size()+config.getPkNames().length+i+1,rsSrc.getObject(config.getUpdateWhereCols()[i]));
			}
		}
		psUpdate.addBatch();
	}
	
	private int getDestCount(SyncConfig config,ResultSet rsSrc,PreparedStatement psDestCount) throws SQLException{
		String[] pkNames=config.getPkNames();
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
	
	private int getDestUpdateCount(SyncConfig config,ResultSet rsSrc,PreparedStatement psDestUpdateCount) throws SQLException{
		String[] pkNames=config.getPkNames();
		for(int i=0;i<pkNames.length;i++){
			psDestUpdateCount.setObject(i+1, rsSrc.getObject(pkNames[i]));
		}
		if(config.getUpdateWhereCols()!=null && !isEmpty(config.getUpdateWhereSql())){
			for(int i=0;i<config.getUpdateWhereCols().length;i++){
				psDestUpdateCount.setObject(config.getPkNames().length+i+1,rsSrc.getObject(config.getUpdateWhereCols()[i]));
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
	
	private void insertOrUpdateRow(SyncConfig config,List<String> colNames,ResultSet rsSrc,PreparedStatement psInsert,PreparedStatement psUpdate,PreparedStatement psDestCount,PreparedStatement psDestUpdateCount) throws SQLException{
		int destCount=getDestCount(config,rsSrc,psDestCount);
		//目标表已有数据
		if(destCount>0){
			updateRow(config,colNames, rsSrc, psUpdate,psDestCount,psDestUpdateCount);//更新
		}else{
			insertRow(config,colNames, rsSrc, psInsert);//插入
		}
	}
	
	private void executeBatch(SyncConfig config,PreparedStatement psInsert,PreparedStatement psUpdate,boolean commitForcely) throws SQLException{
		if(config.scanCount%config.BATCH_NUM==0 || commitForcely ){
			final String tip="已扫描源表：{0},完成：{1} %,目标表更新：{2}，新增：{3}，耗时：{4}秒";
			if(psInsert!=null){
				psInsert.executeBatch();
			}
			if(psUpdate!=null){
				psUpdate.executeBatch();
			}
			config.getDestConn().commit();
			System.out.println(
				MessageFormat.format(
						tip, config.scanCount,
						(config.scanCount*100.0/(config.srcTotal*1.0)),
						config.destUpdate,
						config.destInsert,(System.currentTimeMillis()-config.startTime)/1000)
				);
		}
	}
	
//	private static void setConfigValid(String dsName,String configTable,String uuid,String valid) throws Exception{
//		Connection conn=null;
//		try {
//			conn=DbcpUtil.getConnection(dsName);
//			conn.setAutoCommit(false);
//			String sql="update "+configTable+" set valid=? where uuid=?";
//			PreparedStatement ps=conn.prepareStatement(sql);
//			ps.setString(1, valid);
//			ps.setString(2, uuid);
//			ps.executeUpdate();
//			ps.close();
//			conn.commit();
//		} catch (Exception e) {
//			conn.rollback();
//			throw e;
//		} finally{
//			DbcpUtil.close(conn);
//		}
//	}
	
	private void executeSync() throws SQLException{

		for(SyncConfig config:this.syncConfigList) {
			config.setSrcConn(this.dataSoureMap.get(config.getSrcDsName()).getConnection());
			config.setDestConn(this.dataSoureMap.get(config.getDestDsName()).getConnection());
			checkConfig(config);//同步前检查配置
			
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
				config.getDestConn().setAutoCommit(false);
				if(!isEmpty(config.getSrcTable())){
					config.setSrcSql("select * from "+config.getSrcTable());
				}
				psSrc=config.getSrcConn().prepareStatement(config.getSrcSql());
				System.out.println(config.getSrcSql());
				psCountSrc=config.getSrcConn().prepareStatement(buildCountSql(config.getSrcSql()));
				ResultSet rsCountSrc=psCountSrc.executeQuery();
				rsCountSrc.next();
				config.srcTotal=rsCountSrc.getInt(1);
				System.out.println("共需同步记录数："+config.srcTotal);
				rsCountSrc.close();
				
				ResultSet rsSrc=psSrc.executeQuery();
				colNames=getColNames(rsSrc.getMetaData());
				destInsertSql=buildInsertSql(config,colNames);
				psInsert=config.getDestConn().prepareStatement(destInsertSql);
				
				switch(config.getSyncType()){
				//源表全部复制到目标表
				case INSERT_ALL:
					while(rsSrc.next()){
						insertRow(config,colNames, rsSrc, psInsert);
						executeBatch(config,psInsert, psUpdate,false);
					}
					executeBatch(config,psInsert, psUpdate,true);
					break;
					//源表新记录复制到目标表
				case INSERT_NEW:
					destCountSql=buildDestCountSql(config);
					psDestCount=config.getDestConn().prepareStatement(destCountSql);
					while(rsSrc.next()){
						insertNewRow(config,colNames, rsSrc, psInsert,psDestCount);
						executeBatch(config,psInsert, psUpdate,false);
					}
					executeBatch(config,psInsert, psUpdate,true);
					break;
					
				case UPDATE:
					destUpdateSql=buildUpdateSql(config,colNames);
					destCountSql=buildDestCountSql(config);
					destUpdateCountSql=buildDestUpdateCountSql(config);
					psUpdate=config.getDestConn().prepareStatement(destUpdateSql);
					psDestCount=config.getDestConn().prepareStatement(destCountSql);
					psDestUpdateCount=config.getDestConn().prepareStatement(destUpdateCountSql);
					while(rsSrc.next()){
						updateRow(config,colNames, rsSrc, psUpdate,psDestCount,psDestUpdateCount);
						executeBatch(config,psInsert, psUpdate,false);
					}
					executeBatch(config,psInsert, psUpdate,true);
					break;
					
				case INSERT_UPDATE:
					destUpdateSql=buildUpdateSql(config,colNames);
					destCountSql=buildDestCountSql(config);
					destUpdateCountSql=buildDestUpdateCountSql(config);
					psUpdate=config.getDestConn().prepareStatement(destUpdateSql);
					psDestCount=config.getDestConn().prepareStatement(destCountSql);
					psDestUpdateCount=config.getDestConn().prepareStatement(destUpdateCountSql);
					while(rsSrc.next()){
						insertOrUpdateRow(config,colNames,rsSrc,psInsert,psUpdate,psDestCount,psDestUpdateCount);
						executeBatch(config,psInsert, psUpdate,false);
					}
					executeBatch(config,psInsert, psUpdate,true);
					break;
					
				default:
					//do nothing
					break;
				}
				config.getDestConn().commit();
				System.out.println("同步结束，共耗时：["+(System.currentTimeMillis()-config.startTime)/1000.0+"]秒");
			} catch (SQLException e) {
				try {
					config.getDestConn().rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				throw e;
			} finally{
				if(config.getSrcConn()!=null) {
					try {
						config.getSrcConn().close();
					} catch (Exception e2) {
					}
				}
				if(config.getDestConn()!=null) {
					try {
						config.getDestConn().close();
					} catch (Exception e2) {
					}
				}
			}
			
		}
	}

	public void setConfigTableName(String configTableName) {
		this.configTableName = configTableName;
	}

	public void setConfigDataSource(DataSource configDataSource) {
		this.configDataSource = configDataSource;
	}

	public void setDataSoureMap(Map<String, DataSource> dataSoureMap) {
		this.dataSoureMap = dataSoureMap;
	}
	
}
