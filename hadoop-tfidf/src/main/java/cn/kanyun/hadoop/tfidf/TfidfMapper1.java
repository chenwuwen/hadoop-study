package cn.kanyun.hadoop.tfidf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 第一个MR，计算TF和计算N（微博总数）
 * 
 * @author root
 *
 */
public class TfidfMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] strs = value.toString().trim().split("\t");
		if (strs.length >= 2) {
			String id = strs[0].trim();
			String content = strs[1].trim();
			StringReader sReader = new StringReader(content);
			// 分词
			IKSegmenter ik = new IKSegmenter(sReader, true);
			Lexeme word = null;
			while ((word = ik.next()) != null) {
				String w = word.getLexemeText();
				context.write(new Text(w + "_" + id), new IntWritable(1));
			}
			context.write(new Text("count"), new IntWritable(1));
		}else {
			System.out.println(value.toString()+"--------------");
		}
	}

}
