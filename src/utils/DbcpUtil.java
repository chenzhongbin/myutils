package utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

public class DbcpUtil{
	
	private static final ThreadLocal<Map<String,Connection>> CONNS_MAP=new ThreadLocal<Map<String,Connection>>();
	
	private static final Map<String,DataSource> DS_MAP=new HashMap<String,DataSource>();
	
	private DbcpUtil(){}
	
	static {
		
		try {
			Properties props=new Properties();
			props.setProperty("driverClassName", "com.informix.jdbc.IfxDriver");
			props.setProperty("username", "informix");
			props.setProperty("password", "informix");
			props.setProperty("url", "jdbc:informix-sqli://10.1.9.62:8005/archives:INFORMIXSERVER=adminsoc;DB_LOCALE=zh_cn.gb18030-2000;CLIENT_LOCALE=zh_cn.gb18030-2000;NEWCODESET=gb18030,gb18030-2000,5488");
			props.setProperty("maxActive", "100");
			props.setProperty("initialSize", "10");
			props.setProperty("maxIdle", "20");
			props.setProperty("minIdle", "10");
			props.setProperty("maxWait", "60000");
			DataSource ds=BasicDataSourceFactory.createDataSource(props);
			DS_MAP.put("informix_62", ds);
		} catch (Exception e) {
			System.err.println("创建informix数据源异常！[informix_62]");
			System.err.println(e);
		}
		
		try {
			Properties props=new Properties();
			props.setProperty("driverClassName", "com.informix.jdbc.IfxDriver");
			props.setProperty("username", "informix");
			props.setProperty("password", "informix");
			props.setProperty("url", "jdbc:informix-sqli://10.1.9.61:8002/sysmaster:informixserver=permitsoc;DB_LOCALE=zh_cn.gb18030-2000;CLIENT_LOCALE=zh_cn.gb18030-2000;NEWCODESET=gb18030,gb18030-2000,5488");
			props.setProperty("maxActive", "100");
			props.setProperty("initialSize", "10");
			props.setProperty("maxIdle", "20");
			props.setProperty("minIdle", "10");
			props.setProperty("maxWait", "60000");
			DataSource ds=BasicDataSourceFactory.createDataSource(props);
			DS_MAP.put("informix_61", ds);
		} catch (Exception e) {
			System.err.println("创建informix数据源异常！[informix_61]");
			System.err.println(e);
		}
		
		try {
			Properties props=new Properties();
			props.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
			props.setProperty("username", "dagl");
			props.setProperty("password", "xdrcft56");
			props.setProperty("url", "jdbc:oracle:thin:@10.1.8.131:1521:orcl");
			props.setProperty("maxActive", "100");
			props.setProperty("initialSize", "10");
			props.setProperty("maxIdle", "20");
			props.setProperty("minIdle", "10");
			props.setProperty("maxWait", "60000");
			DataSource ds=BasicDataSourceFactory.createDataSource(props);
			DS_MAP.put("oracle_dagl", ds);
		} catch (Exception e) {
			System.err.println("创建oracle数据源异常！[oracle_dagl]");
			System.err.println(e);
		}
		
//		try {
//			Properties props=new Properties();
//			props.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
//			props.setProperty("username", "appuser");
//			props.setProperty("password", "xdrcft56");
//			props.setProperty("url", "jdbc:oracle:thin:@172.28.129.21:1521:sck1");
//			props.setProperty("maxActive", "100");
//			props.setProperty("initialSize", "10");
//			props.setProperty("maxIdle", "20");
//			props.setProperty("minIdle", "10");
//			props.setProperty("maxWait", "60000");
//			DataSource ds=BasicDataSourceFactory.createDataSource(props);
//			DS_MAP.put("oracle_me_172", ds);
//		} catch (Exception e) {
//			System.err.println("创建oracle数据源异常！[172]");
//			System.err.println(e);
//		}
//		
//		try {
//			Properties props=new Properties();
//			props.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
//			props.setProperty("username", "appuser");
//			props.setProperty("password", "xdrcft56");
//			props.setProperty("url", "jdbc:oracle:thin:@10.253.254.73:1521:web1");
//			props.setProperty("maxActive", "100");
//			props.setProperty("initialSize", "10");
//			props.setProperty("maxIdle", "20");
//			props.setProperty("minIdle", "10");
//			props.setProperty("maxWait", "60000");
//			DataSource ds=BasicDataSourceFactory.createDataSource(props);
//			DS_MAP.put("oracle_me_10", ds);
//		} catch (Exception e) {
//			System.err.println("创建oracle数据源异常！[10]");
//			System.err.println(e);
//		}
//		
//		try {
//			Properties props=new Properties();
//			props.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
//			props.setProperty("username", "appuser");
//			props.setProperty("password", "xdrcft56");
//			props.setProperty("url", "jdbc:oracle:thin:@10.253.254.5:1521:web");
//			props.setProperty("maxActive", "100");
//			props.setProperty("initialSize", "10");
//			props.setProperty("maxIdle", "20");
//			props.setProperty("minIdle", "10");
//			props.setProperty("maxWait", "60000");
//			DataSource ds=BasicDataSourceFactory.createDataSource(props);
//			DS_MAP.put("oracle_me_10_test", ds);
//		} catch (Exception e) {
//			System.err.println("创建oracle数据源异常！[10_test]");
//			System.err.println(e);
//		}
//		
//		try {
//			Properties props=new Properties();
//			props.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
//			props.setProperty("username", "appuser");
//			props.setProperty("password", "xdrcft56");
//			props.setProperty("url", "jdbc:oracle:thin:@localhost:1521:demo");
//			props.setProperty("maxActive", "100");
//			props.setProperty("initialSize", "10");
//			props.setProperty("maxIdle", "20");
//			props.setProperty("minIdle", "10");
//			props.setProperty("maxWait", "60000");
//			DataSource ds=BasicDataSourceFactory.createDataSource(props);
//			DS_MAP.put("oracle_me_localhost", ds);
//		} catch (Exception e) {
//			System.err.println("创建oracle数据源异常！[oracle_me_localhost]");
//			System.err.println(e);
//		}
//		
//		try {
//			Properties props=new Properties();
//			props.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
//			props.setProperty("username", "appuser");
//			props.setProperty("password", "xdrcft56");
//			props.setProperty("url", "jdbc:oracle:thin:@172.28.129.14:1521:sck");
//			props.setProperty("maxActive", "100");
//			props.setProperty("initialSize", "10");
//			props.setProperty("maxIdle", "20");
//			props.setProperty("minIdle", "10");
//			props.setProperty("maxWait", "60000");
//			DataSource ds=BasicDataSourceFactory.createDataSource(props);
//			DS_MAP.put("oracle_me_172_test", ds);
//		} catch (Exception e) {
//			System.err.println("创建oracle数据源异常！[oracle_me_172_test]");
//			System.err.println(e);
//		}
		
		//mysql
//		try {
//			Properties props=new Properties();
//			props.setProperty("driverClassName", "com.mysql.jdbc.Driver");
//			props.setProperty("username", "root");
//			props.setProperty("password", "36987");
//			props.setProperty("url", "jdbc:mysql://localhost:3366/chen?useUnicode=true&characterEncoding=utf-8");
//			props.setProperty("maxActive", "100");
//			props.setProperty("initialSize", "10");
//			props.setProperty("maxIdle", "20");
//			props.setProperty("minIdle", "10");
//			props.setProperty("maxWait", "60000");
//			mysqlDs=BasicDataSourceFactory.createDataSource(props);
//		} catch (Exception e) {
//			System.err.println("创建mysql数据源异常！");
//			System.err.println(e);
//		}
		
	}
	
	public static Connection getConnection(String type,boolean autocommit) throws SQLException{
		Map<String,Connection> map=CONNS_MAP.get();
		if(map==null) {
			map =new HashMap<String,Connection>();
			CONNS_MAP.set(map);
		}
		Connection conn=map.get(type);
		if(conn==null){
			conn=DS_MAP.get(type).getConnection();
			if(conn==null) throw new RuntimeException("获取数据库连接失败！");//TODO 这代码没必要吧
			map.put(type, conn);
		}
		conn.setAutoCommit(autocommit);
		return conn;
	}
	
	public static void rollback(String type){
		Connection conn=null;
		try {
			Map<String,Connection> map=CONNS_MAP.get();
			if(map==null) {
				return;
			}
			conn=map.get(type);
			if(conn==null){
				return;
			}
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection(String type) throws SQLException{
		return getConnection(type,false);
	}
	
	public static void release(String type){
		Connection conn=null;
		try {
			Map<String,Connection> map=CONNS_MAP.get();
			if(map==null) {
				return;
			}
			conn=map.get(type);
			if(conn==null){
				return;
			}
			map.remove(type);
			conn.setAutoCommit(true);
			if(map.isEmpty()) CONNS_MAP.remove();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		Thread.sleep(1000);
	}
	
}