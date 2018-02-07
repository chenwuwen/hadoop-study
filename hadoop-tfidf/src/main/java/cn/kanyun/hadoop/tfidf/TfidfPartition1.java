package cn.kanyun.hadoop.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 第一个MR自定义分区
 * 
 * @author root
 *
 */
public class TfidfPartition1 extends HashPartitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
		if (key.equals(new Text("count"))) {
			// 最后一个统计微博总数
			return 3;
		} else {
			return super.getPartition(key, value, numReduceTasks - 1);
		}
	}

}
