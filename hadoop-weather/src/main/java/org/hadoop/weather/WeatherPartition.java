package org.hadoop.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WeatherPartition extends HashPartitioner<Weather, IntWritable> {

	@Override
	public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
		// 写Partition规则：1：满足业务，2:越简单越好
		return (key.getYear() - 1949) % numReduceTasks;
		// 默认的Partition的规则：取key的hash值模上reduce个数
		// return super.getPartition(key, value, numReduceTasks);
	}

}
