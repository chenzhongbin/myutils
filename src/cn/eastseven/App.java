package cn.eastseven;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import cn.eastseven.model.Column;
import cn.eastseven.model.Table;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * Hello world!
 * 
 */
public class App {

	static Logger logger = Logger.getLogger(App.class.getName());

	public static void main(String[] args) throws Exception {
		DbService dbService = new DbServiceImpl();
		List<Table> tables = dbService.getTablesFromJdbc();
		logger.debug(tables);
		generateJavaBeanFile(tables);
	}

	private static void generateJavaBeanFile(List<Table> tables) throws IOException, TemplateException, ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration("resources/generator.properties");
        final String path = conf.getString("dest.dir");
        final String packageName = conf.getString("class.package");
        File folder=new File(path);
        if(folder.isFile()){
        	throw new RuntimeException("应该要创建文件夹，但有一个同名的文件！");
        }
        if(!folder.exists()){
        	folder.mkdir();
        }
		Writer out = new OutputStreamWriter(System.out);
		for (Table table : tables) {
			freemarker.template.Configuration cfg = new freemarker.template.Configuration(); 
			cfg.setDirectoryForTemplateLoading(new File("resources/freemarker"));
			cfg.setObjectWrapper(new DefaultObjectWrapper());
			Template temp = cfg.getTemplate("javabean.ftl");
			Map<String, Object> root = Maps.newHashMap();

			String tableName = table.getName();
			String className = getName(tableName);
			
			root.put("package", packageName);
			root.put("className", className);
			
			List<Map<String, Object>> properties = Lists.newArrayList();
			List<Map<String, Object>> methods = Lists.newArrayList();
			for(Column c : table.getColumns()) {
				Map<String, Object> e = Maps.newHashMap();
				Map<String, Object> m = Maps.newHashMap();
				
				String columnName = getName(c.getName());
				String methodName = columnName;
				columnName = columnName.substring(0,1).toLowerCase() + columnName.substring(1, columnName.length());
				e.put("name", columnName);
				e.put("type", getType(c.getDataType()));
				
				m.put("get", "get"+methodName);
				m.put("set", "set"+methodName);
				m.put("name", columnName);
				m.put("columnName", c.getName());
				m.put("type", getType(c.getDataType()));
				
				logger.debug(c);
				properties.add(e);
				methods.add(m);
			}
			root.put("properties", properties);
			root.put("methods", methods);
			root.put("tableName", table.getName());
			
			temp.process(root, out);
			out.flush();
			
			FileWriter writer = new FileWriter(new File(folder,className+".java"));
			temp.process(root, writer);
			writer.flush();
			writer.close();
		}
		out.close();
	}

	/**
	 * 格式化字段名称，去掉下划线，驼峰格式
	 * @param target 输入
	 * @return 输出
	 */
	public static String getName(String target) {
		String[] names = target.toLowerCase().split("_");
		String className = "";
		for (String name : names) {
			className += name.substring(0, 1).toUpperCase() + name.substring(1, name.length());
		}
		return className;
	}

	private static String getType(int type) {
		switch (type) {
		// String
		case Types.CHAR:
			return String.class.getName();
		case Types.VARCHAR:
			return String.class.getName();
		case Types.LONGNVARCHAR:
			return String.class.getName();
		case Types.LONGVARCHAR:
			return String.class.getName();
			
		// Number
		case Types.INTEGER:
			return Integer.class.getName();
		case Types.TINYINT:
			return Integer.class.getName();
		case Types.SMALLINT:
			return Integer.class.getName();
		case Types.DECIMAL:
			return Long.class.getName();
		case Types.DOUBLE:
			return Double.class.getName();
		case Types.FLOAT:
			return Float.class.getName();
		case Types.REAL:
			return Double.class.getName(); 
			
		//	Date
		case Types.DATE:
			return Date.class.getName();
		case Types.TIME:
			return Date.class.getName();
		case Types.TIMESTAMP:
			return Date.class.getName();
		
		// LOB
		case Types.BLOB:
			return Object.class.getName();
		case Types.CLOB:
			return String.class.getName();
		case Types.LONGVARBINARY:
			return Object.class.getName();
		case Types.VARBINARY:
			return Object.class.getName();
			
		// Other
		case Types.BOOLEAN:
			return Boolean.class.getName();
		case Types.BIT:
			return Boolean.class.getName();
			
		default:
			logger.warn("type = " + type);
			return "";
		}
	}
}
