package org.hadoop.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Weather, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Weather arg0, Iterable<IntWritable> arg1,
			Reducer<Weather, IntWritable, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int flag = 0;
		for (IntWritable intWritable : arg1) {
			flag++;
			if (flag > 2) {
				break;
			}
			String msg = arg0.getYear() + "-" + arg0.getMouth() + "-" + intWritable.get();
			arg2.write(new Text(msg), NullWritable.get());
		}
	}

}
