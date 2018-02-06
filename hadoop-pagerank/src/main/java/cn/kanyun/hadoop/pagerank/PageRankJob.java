package cn.kanyun.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 代码中设置用户名,用以连接hadoop集群进行操作
		// System.setProperty("HADOOP_USER_NAME", "root");

		// Configuration默认加载src目录下的配置文件
		Configuration configuration = new Configuration();
		// configuration.set("fs.defaultFs", "hdfs://slave1:8020");
		// configuration.set("yarn.resourcemanager.hostname", "slave3");

		Job job = Job.getInstance(configuration);

		// 收链指数
		double d = 0.001;
		int i = 0;
		while (true) {
			i++;
			configuration.setInt("runcount", i);
			// 指定程序入口
			job.setJarByClass(PageRankJob.class);
			FileSystem fSystem = FileSystem.get(configuration);
			job.setMapperClass(PageRankMapper.class);
			job.setJobName("PR" + i);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(PageRankReducer.class);
			// 格式化一行数据
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			Path inputPath = new Path("/pagerank/input/pagerank");
			if (i > 1) {
				inputPath = new Path("/pagerank/output/pr" + (i - 1));
			}

			FileInputFormat.addInputPath(job, inputPath);

			Path outputPath = new Path("/pagerank/outputPath/pr" + i);

			if (fSystem.exists(outputPath)) {
				fSystem.delete(outputPath, true);
			}

			FileOutputFormat.setOutputPath(job, outputPath);

			if (job.waitForCompletion(true)) {
				System.out.println("Job success.............");
				// 获取计数器中的数值
				long sum = job.getCounters().findCounter(MyCountEnum.my).getValue();
				System.out.println("SUM:" + sum);
				double avgd = sum / 4000.0;
				if (avgd < d) {
					// 收链
					break;
				}

			} else {
				System.out.println("Job error.............");
			}
		}

	}
}
