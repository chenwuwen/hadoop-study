package cn.kanyun.hadoop.commend.step1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommendMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] strings = StringUtils.split(value.toString(),' ');
		Format format = new Format();
		for (int i = 0; i < strings.length; i++) {
			// 直接好友关系
			String s1 = format.strFormat(strings[0], strings[i]);
			context.write(new Text(s1), new Integer(0));
			for (int j = i + 1; j < strings.length; j++) {
				// 全部好友关系
				String s2 = format.strFormat(strings[i], strings[j]);
				context.write(new Text(s2), new Integer(1));
			}
		}
	}

}
