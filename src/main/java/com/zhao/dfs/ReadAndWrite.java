package com.zhao.dfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ReadAndWrite {
	private ApplicationContext ctx;
	private FileSystem fileSystem;
	public ReadAndWrite() {
		// 加载配置信息
		ctx = new ClassPathXmlApplicationContext("beans.xml");
		fileSystem = (FileSystem) ctx.getBean("fileSystem");
	}
	@Scheduled(cron="20 * * * * ?")
	public void testText() throws IllegalArgumentException, IOException {
		System.out.println("开始执行！");
		FSDataInputStream in = fileSystem.open(new Path("/user/admin/output/part-r-00000"));
		System.out.println("读取文件");
		IOUtils.copyBytes(in, System.out, 1024);
		System.out.println("执行结束");
		in.close();
	}
	@Scheduled(cron="10 * * * * ?")
	public void testMkdirs() throws IllegalArgumentException, IOException {
		fileSystem.mkdirs(new Path("/SpringHDFS"));
	}
	
}
