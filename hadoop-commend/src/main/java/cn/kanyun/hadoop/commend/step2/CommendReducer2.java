package cn.kanyun.hadoop.commend.step2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CommendReducer2 extends Reducer<Commend, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Commend arg0, Iterable<IntWritable> arg1,
			Reducer<Commend, IntWritable, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for (IntWritable i : arg1) {
			String msg = arg0.getName1() + "-" + arg0.getName2() + ":" + i.get();
			arg2.write(new Text(msg), NullWritable.get());
		}
	}

}
