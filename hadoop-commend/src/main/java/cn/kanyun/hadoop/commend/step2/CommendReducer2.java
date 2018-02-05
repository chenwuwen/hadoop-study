package cn.kanyun.hadoop.commend.step2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommendReducer2 extends Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.reduce(arg0, arg1, arg2);
	}

}
