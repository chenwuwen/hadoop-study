package cn.kanyun.hadoop.tfidf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 第一个MR，计算TF和计算N（微博总数）
 * 
 * @author root
 *
 */
public class TfidfMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	// 存放微博总数
	public static Map<String, Integer> cmap = null;
	// 存放df
	public static Map<String, Integer> dmap = null;

	/**
	 * 此方法在map方法执行前执行，本例中用来初始化cmap和dmap
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("******************");
		if (cmap == null || cmap.size() == 0 || dmap == null || dmap.size() == 0) {
			// 拿到内存缓存器中数据文件
			URI[] ss = context.getCacheFiles();
			if (ss != null) {
				for (int i = 0; i < ss.length; i++) {
					URI uri = ss[i];
					// 微博总条数文件
					if (uri.getPath().equals("part-r-00003")) {
						Path path = new Path(uri.getPath());
						System.out.println(uri.getPath() + "   " + path.getName());
						BufferedReader br = new BufferedReader(new FileReader(path.getName()));
						String line = br.readLine();
						if (line.startsWith("count")) {
							String[] ls = line.split("\t");
							cmap = new HashMap<String, Integer>();
							cmap.put(ls[0], Integer.parseInt(ls[1].trim()));

						}
						br.close();
					} else if (uri.getPath().endsWith("part-r-00000")) { // 微博的df
						dmap = new HashMap<String, Integer>();
						Path path = new Path(uri.getPath());
						System.out.println("-------------" + uri.getPath());
						BufferedReader br = new BufferedReader(new FileReader(path.getName()));
						String line;
						while ((line = br.readLine()) != null) {
							String[] ls = line.split("\t");
							dmap.put(ls[0], Integer.parseInt(ls[1].trim()));
						}
						br.close();
					}
				}
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit fs = (FileSplit) context.getInputSplit();
		if (!fs.getPath().getName().contains("part-r-00003")) {
			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				int tf = Integer.parseInt(v[1].trim()); // tf值
				String[] ss = v[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0];
					String id = ss[1];
					double s = tf * Math.log(cmap.get("count") / dmap.get(w));
					NumberFormat nf = NumberFormat.getInstance();
					// 设置小数值到后5位
					nf.setMaximumFractionDigits(5);
					context.write(new Text(id), new Text(w + ":" + nf.format(s)));
				}

			}

		} else {
			System.out.println(value.toString() + "------------");
		}
	}

}
