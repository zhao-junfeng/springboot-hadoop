package com.zhao.dfs;
/**
 * 
 * @ClassName:  SpringHadoopApp   
 * @Description:  TODO(使用Spring )
 * @author: 赵俊锋
 * @date:   2019年5月29日 下午5:02:22   
 *
 */

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
//@Component
public class SpringHadoopApp {

	private ApplicationContext ctx;
	private FileSystem fileSystem;
	
	@Before
	public void setUp() {
		ctx = new ClassPathXmlApplicationContext("beans.xml");
		fileSystem = (FileSystem) ctx.getBean("fileSystem");
	}
	
	@After
	public void tearDown() throws IOException {
		ctx = null;
		fileSystem.close();
	}
	
	/**
	 * 在HDFS上创建一个目录
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 */
	//@Test
	public void testMkdirs() throws IllegalArgumentException, IOException {
		fileSystem.mkdirs(new Path("/SpringHDFS"));
	}
	
	/**
	 * 读取HDFS上的文件内容
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 */
	@Test
	public void testText() throws IllegalArgumentException, IOException {
		System.out.println("开始执行！");
		FSDataInputStream in = fileSystem.open(new Path("/user/admin/output/part-r-00000"));
		System.out.println("读取文件");
		IOUtils.copyBytes(in, System.out, 1024);
		System.out.println("执行结束");
		in.close();
	}
	
}
