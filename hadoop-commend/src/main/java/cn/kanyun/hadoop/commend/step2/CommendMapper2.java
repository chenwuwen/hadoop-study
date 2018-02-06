package cn.kanyun.hadoop.commend.step2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommendMapper2 extends Mapper<LongWritable, Text, Commend, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Commend, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] strs = value.toString().split("-");
		Commend commend1 = new Commend();
		commend1.setName1(strs[0]);
		commend1.setName2(strs[1]);
		commend1.setHot(Integer.parseInt(strs[2]));
		context.write(commend1, new IntWritable(commend1.getHot()));
		Commend commend2 = new Commend();
		commend2.setName1(strs[1]);
		commend2.setName2(strs[0]);
		commend2.setHot(Integer.parseInt(strs[2]));
		context.write(commend2, new IntWritable(commend2.getHot()));
	}

}
