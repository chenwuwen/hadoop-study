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

public class TfidfJob3 {
	public static void main(String[] args) throws IOException {
		// 代码中设置用户名,用以连接hadoop集群进行操作
		// System.setProperty("HADOOP_USER_NAME", "root");

		// Configuration默认加载src目录下的配置文件
		Configuration configuration = new Configuration();
		// configuration.set("fs.defaultFs", "hdfs://slave1:8020");
		// configuration.set("yarn.resourcemanager.hostname", "slave3");

		Job job = Job.getInstance(configuration);
		// 把微博总数加载进内存(注意：此job需要提交到服务器执行)
		job.addCacheFile(new Path("/tfidf/output2").toUri());
		// 把df加载进内存(注意：此job需要提交到服务器执行)
		job.addCacheFile(new Path("/tfidf/output2").toUri());
		// 指定程序入口
		job.setJarByClass(TfidfJob3.class);
		job.setJobName("weibo3");
		job.setMapperClass(TfidfMapper3.class);
		job.setReducerClass(TfidfReducer3.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(configuration);
		Path inputPath = new Path("/tfidf/output2");
		FileInputFormat.addInputPath(job, inputPath);
		Path outputPath = new Path("/tfidf/output3/");
		FileOutputFormat.setOutputPath(job, outputPath);
	}
}
