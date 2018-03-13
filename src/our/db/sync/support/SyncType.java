package our.db.sync.support;

public enum SyncType {
	INSERT_ALL, 	// 全部插入目标表，不考虑目标表是否已有重复的记录
	INSERT_NEW, 	// 同步新的记录到目标表
	UPDATE, 		// 更新目标表记录
	INSERT_UPDATE	// 新增数据插入，已有数据更新
}