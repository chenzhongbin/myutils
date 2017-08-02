package mine.chen.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TextUtil {
    public static StringBuffer readToBuffer(InputStream is) throws IOException {
    	StringBuffer buffer=new StringBuffer();
    	String line; // 用来保存每行读取的内容
        BufferedReader reader = null;
        try {
        	reader = new BufferedReader(new InputStreamReader(is));
        	line = reader.readLine(); // 读取第一行
        	while (line != null) { // 如果 line 为空说明读完了
        		buffer.append(line); // 将读到的内容添加到 buffer 中
        		buffer.append("\n"); // 添加换行符
        		line = reader.readLine(); // 读取下一行
        	}
        	reader.close();
        	return buffer;
		} catch (IOException e) {
			throw e;
		} finally{
			try {
				if(reader!=null) reader.close();
			} catch (IOException ex) {
			}
			try {
				if(is!=null) is.close();
			} catch (IOException ex) {
			}
		}
    }
}
