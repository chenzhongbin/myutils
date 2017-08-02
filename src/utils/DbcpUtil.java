package utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

public class DbcpUtil{
	
	private static final ThreadLocal<Map<String,Connection>> CONNS_MAP=new ThreadLocal<Map<String,Connection>>();
	
	private static final Map<String,DataSource> DS_MAP=new HashMap<String,DataSource>();
	
	private static final Map<String,Properties> PROPS_MAP=new HashMap<String,Properties>();
	
	private static final String PROP_FILE_PATH="resources/jdbc.dbcputil.properties";
	
	private DbcpUtil(){}
	
	static {
		init();
	}
	
	private static DataSource createDataSource(String dsName){
		try {
			Properties props=PROPS_MAP.get(dsName);
			return BasicDataSourceFactory.createDataSource(props);
		} catch (Exception e) {
			throw new RuntimeException("创建数据源异常："+dsName+"!");
		}
	}
	
	private static void init(){
		Properties prop = PropertiesUtil.getProperties(PROP_FILE_PATH);
		Iterator<String> it=prop.stringPropertyNames().iterator();
		while(it.hasNext()){
			String key=it.next();
			String[] words=key.split("\\.");
			if(words.length!=2){
				throw new RuntimeException("配置文件属性名格式错误：["+key+"]。要求格式：[xxxx.xxxx]。");
			}
			String propKey=words[0];
			String propValue=words[1];
			Properties subProperties=PROPS_MAP.get(propKey);
			if(subProperties==null){
				subProperties=new Properties();
				PROPS_MAP.put(propKey, subProperties);
			}
			subProperties.put(propValue, prop.getProperty(key));
		}
	}
	
	public static Connection getConnection(String dataSourceName,boolean autocommit) throws Exception{
		Map<String,Connection> map=CONNS_MAP.get();
		if(map==null) {
			map =new HashMap<String,Connection>();
			CONNS_MAP.set(map);
		}
		Connection conn=map.get(dataSourceName);
		if(conn==null||conn.isClosed()){
			DataSource ds=DS_MAP.get(dataSourceName);
			if(ds==null){
				ds=createDataSource(dataSourceName);
				DS_MAP.put(dataSourceName, ds);
			}
			conn=ds.getConnection();
			if(conn==null) throw new RuntimeException("获取数据库连接失败！");
			map.put(dataSourceName, conn);
		}
		conn.setAutoCommit(autocommit);
		return conn;
	}
	
	
	public static void rollback(String ... dsNames){
		for(String dsName:dsNames){
			rollback(dsName);
		}
	}
	
	public static void rollback(String dsName){
		Connection conn=null;
		try {
			Map<String,Connection> map=CONNS_MAP.get();
			if(map==null) {
				return;
			}
			conn=map.get(dsName);
			if(conn==null){
				return;
			}
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void rollback(){
		Set<String> keySet=CONNS_MAP.get().keySet();
		Iterator<String> iter=keySet.iterator();
		while(iter.hasNext()){
			String dsName=iter.next();
			rollback(dsName);
		}
	}
	
	public static Connection getConnection(String dsName){
		try {
			return getConnection(dsName,false);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	public static void release(String dsName){
		Connection conn=null;
		try {
			Map<String,Connection> map=CONNS_MAP.get();
			if(map==null) {
				return;
			}
			conn=map.get(dsName);
			if(conn==null){
				return;
			}
			map.remove(dsName);
			conn.setAutoCommit(true);
			if(map.isEmpty()) CONNS_MAP.remove();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			DbcpUtil.close(conn);
		}
	}
	
	public static void release(String ... dsNames){
		for(String dsName:dsNames){
			release(dsName);
		}
	}
	
	public static void release(){
		Set<String> keySet=CONNS_MAP.get().keySet();
		Iterator<String> iter=keySet.iterator();
		List<String> dsNames=new ArrayList<String>();
		while(iter.hasNext()){
			String dsName=iter.next();
			dsNames.add(dsName);
		}
		for(String dsName:dsNames){
			release(dsName);
		}
	}
	
	public static void close(AutoCloseable...objs){
		for(AutoCloseable o:objs){
			try {
				if(o!=null) o.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		init();
		System.out.println(PROPS_MAP);
	}
	
}