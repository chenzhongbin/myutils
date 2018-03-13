package our.db.sync.test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import our.db.sync.Synchronizer;

public class TestCase {
	public static void main(String[] args) throws Exception {
		ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext.xml" });

		Synchronizer tableSynchronizer = ctx.getBean(Synchronizer.class);
		tableSynchronizer.synchronize();

		((ClassPathXmlApplicationContext) ctx).close();
	}
}
