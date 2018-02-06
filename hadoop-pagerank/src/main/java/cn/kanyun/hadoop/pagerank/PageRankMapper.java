package cn.kanyun.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int runcount = context.getConfiguration().getInt("runcount", 1);
		// A B D
		String page = key.toString();
		Node node = null;
		if (runcount == 1) {
			// 第一次计算，初始化pr值为1.0
			node = node.fromMR("1.0" + "\t" + value.toString());
		} else {
			node = node.fromMR(value.toString());
		}
		// 将计算前的数据输出，Reducer计算做差值
		// key：A value: 1.0 B D
		context.write(new Text(page), new Text(node.toString()));

		if (node.containsAdjacentNodes()) {
			double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
			for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
				String outPage = node.getAdjacentNodeNames()[i];
				// B:0.5
				// D:0.5
				context.write(new Text(outPage), new Text(outValue + ""));
			}
		}
	}

}
