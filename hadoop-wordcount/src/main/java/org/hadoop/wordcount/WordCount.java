package org.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 代码中设置用户名,用以连接hadoop集群进行操作
		// System.setProperty("HADOOP_USER_NAME", "root");
		
		// Configuration默认加载src目录下的配置文件
		Configuration configuration = new Configuration();
		// configuration.set("fs.defaultFs", "hdfs://slave1:8020");
		// configuration.set("yarn.resourcemanager.hostname", "slave3");

		Job job = Job.getInstance(configuration);

		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(WCReducer.class);

		FileSystem fSystem = FileSystem.get(configuration);

		Path outputPath = new Path("/wc/outputPath");

		if (fSystem.exists(outputPath)) {
			fSystem.delete(outputPath, true);
		}
		Path inputPath = new Path("/wc/input");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (job.waitForCompletion(true)) {
			System.out.println("Job success.............");
		} else {
			System.out.println("Job error.............");
		}

	}

}
