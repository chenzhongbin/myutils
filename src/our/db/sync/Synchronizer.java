package our.db.sync;

import our.db.sync.listener.SynchronizeListener;

/**
 * 数据同步器
 * @author chenzhongbin
 * 
 */
public interface Synchronizer {
	
	/**
	 * 同步数据
	 * @param src
	 * @param destination
	 */
	void synchronize() throws Exception;
	
	/**
	 * 添加监听器用于接收同步消息
	 * @param listener
	 */
	void addListener(SynchronizeListener listener);
	
	/**
	 * 移除监听器
	 * @param listener
	 */
	void removeListener(SynchronizeListener listener);
}
