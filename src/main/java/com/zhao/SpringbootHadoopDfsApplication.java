package com.zhao;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 
 * @ClassName:  SpringbootHadoopDfsApplication   
 * @Description:  TODO()
 * @author: 赵俊锋
 * @date:   2019年5月30日 下午2:02:24   
 * @program:  hadoop-train
 * @description:使用spring boot来访问HDFS
 */

@SpringBootApplication
@EnableScheduling
public class SpringbootHadoopDfsApplication implements CommandLineRunner{
	
	@Autowired
	FsShell fsShell;//用于执行hdfs shell命令的对象
	
	
	
	@Override
	public void run(String... args) throws Exception {
		// 查看根目录下的所有文件
		for(FileStatus fileStatus : fsShell.ls("/")) {
			System.out.println(">"+fileStatus.getPath());
		}
		
	}
	public static void main(String[] args) {
		SpringApplication.run(SpringbootHadoopDfsApplication.class, args);
	}
}
