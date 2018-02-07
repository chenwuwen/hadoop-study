package cn.kanyun.hadoop.tfidf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import com.google.common.base.Strings;

/**
 * 第一个MR，计算TF和计算N（微博总数）
 * 
 * @author root
 *
 */
public class TfidfMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 获取当前mapper task的数据片段（split）
		FileSplit fs = (FileSplit) context.getInputSplit();
		if (!fs.getPath().getName().contains("part-t-00003")) {
			String[] strs = value.toString().trim().split("\t");
			if (strs.length >= 2) {
				String[] ss = strs[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0];
					context.write(new Text(w), new IntWritable(1));
				}
			} else {
				System.out.println(value.toString() + "------------");
			}

		}
	}

}
