package our.db.sync.support;

import java.sql.Connection;

import org.springframework.util.StringUtils;

public class SyncConfig {

	private String uuid;
	private Connection srcConn;
	private Connection destConn;
	private String srcDsName;
	private String destDsName;
	private SyncType syncType;// 同步类型
	private String srcTable;// 来源表
	private String srcSql;// 来源查询语句，优先级<来源表
	private String destTable;// 目标表
	private String[] pkNames;// 主键名，支持联合主键

	private String updateWhereSql;
	private String[] updateWhereCols = new String[] {};

	int srcTotal = 0;
	int destUpdate = 0;
	int destInsert = 0;
	int scanCount = 0;
	final int BATCH_NUM = 1000;
	long startTime;

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
		if (updateWhereCols == null || "".equals(updateWhereCols.trim())) {
			this.updateWhereCols = new String[] {};
		} else {
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
		if (!StringUtils.isEmpty(pkNames)) {
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

	public void setSrcConn(Connection srcConn) {
		this.srcConn = srcConn;
	}

	public void setDestConn(Connection destConn) {
		this.destConn = destConn;
	}

}
