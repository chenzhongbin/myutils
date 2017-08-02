package utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {
	
	public static Properties getProperties(String filePath){
		Properties properties = new Properties();
		InputStream in=null;
		try {
			in = new BufferedInputStream(new FileInputStream(filePath));
			properties.load(in);
			in.close();
		} catch(Exception e){
			e.printStackTrace();
			return null;
		} finally{
			try {
				if(in!=null)
					in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return properties;
	}
	
	public static Map<String, String> getPropertyMap(String filePath) {
		LinkedHashMap<String, String> propertiesMap = new LinkedHashMap<String, String>();
		Properties properties = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(filePath));
			properties.load(in);
			in.close();
			Enumeration<?> propKeys = properties.propertyNames();
			while (propKeys.hasMoreElements()) {
				String propName = (String) propKeys.nextElement();
				String propValue = properties.getProperty(propName);
				propertiesMap.put(propName, propValue);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return propertiesMap;
	}
}
