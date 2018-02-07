package cn.kanyun.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TfidfReducer3 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		for (Text t : arg1) {
			sb.append(t.toString() + "\t");
		}
		arg2.write(arg0, new Text(sb.toString()));
	}

}
