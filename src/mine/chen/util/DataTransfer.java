package mine.chen.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class DataTransfer {
	
	@Resource(name="dsYnzyRs")
	private DataSource dsYnzyRs;
	
	@Resource(name="dsLocal")
	private DataSource dsLocal;
	
	public void transfer(){
		Connection connLocal=null;
		Connection connYnzRs=null;
		try {
			connYnzRs=dsYnzyRs.getConnection();
			connLocal=dsLocal.getConnection();
			PreparedStatement psDIC=connLocal.prepareStatement("select * from DIC");
			PreparedStatement psTYPE=connYnzRs.prepareStatement("select * from RS_DIC_TYPE");
			ResultSet rsType=psTYPE.executeQuery();
			Map<String,Long> map=new HashMap<String,Long>();
			while(rsType.next()){
				String type=rsType.getString("TYPE");
				Long typeId=rsType.getLong("ID");
				map.put(type, typeId);
			}
			PreparedStatement psITEM=connYnzRs.prepareStatement("insert into RS_DIC_ITEM(TYPE,CODE,VALUE,TYPE_ID) values(?,?,?,?)");
			ResultSet rsLocal=psDIC.executeQuery();
			while(rsLocal.next()){
				String type=rsLocal.getString("COLUMN_NAME_CN");
				String code=rsLocal.getString("CODE");
				String value=rsLocal.getString("TEXT");
				psITEM.setString(1, type);
				psITEM.setString(2, code);
				psITEM.setString(3, value);
				psITEM.setLong(4, map.get(type));
				psITEM.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			
			try {
				connLocal.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			try {
				connYnzRs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		ApplicationContext ctx=new ClassPathXmlApplicationContext("applicationContext.xml");
		DataTransfer ins=ctx.getBean(DataTransfer.class);
		ins.transfer();
		((ClassPathXmlApplicationContext) ctx).close();
	}
	
}
