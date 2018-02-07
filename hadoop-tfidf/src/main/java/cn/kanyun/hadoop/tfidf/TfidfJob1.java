package cn.kanyun.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class TfidfJob1 {
	public static void main(String[] args) throws IOException {
		// 代码中设置用户名,用以连接hadoop集群进行操作
		// System.setProperty("HADOOP_USER_NAME", "root");

		// Configuration默认加载src目录下的配置文件
		Configuration configuration = new Configuration();
		// configuration.set("fs.defaultFs", "hdfs://slave1:8020");
		// configuration.set("yarn.resourcemanager.hostname", "slave3");

		Job job = Job.getInstance(configuration);

		// 指定程序入口
		job.setJarByClass(TfidfJob1.class);
		job.setJobName("weibo1");
		job.setNumReduceTasks(4);
		job.setPartitionerClass(TfidfPartition1.class);
		job.setMapperClass(TfidfMapper1.class);
		job.setReducerClass(TfidfReducer1.class);
		job.setCombinerClass(TfidfReducer1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileSystem fs = FileSystem.get(configuration);
		Path inputPath = new Path("/tfidf/input/tfidf");
		FileInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("/tfidf/output/");
		FileOutputFormat.setOutputPath(job, outputPath);
	}
}
