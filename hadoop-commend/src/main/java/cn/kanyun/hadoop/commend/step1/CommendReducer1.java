package cn.kanyun.hadoop.commend.step1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommendReducer1 extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		boolean flag = true;
		// 亲密度
		int sum = 0;
		for (IntWritable i : arg1) {
			if (i.get() == 0) {
				flag = false;
				break;
			}
			sum++;
		}
		// 真正的二度关系
		if (flag) {
			String msg = arg0.toString() + sum;
			arg2.write(new Text(msg), NullWritable.get());
		}
	}

}
