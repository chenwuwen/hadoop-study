package cn.kanyun.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		double sum = 0.0;
		Node sourceNode = null;
		for (Text t : arg1) {
			Node node = Node.fromMR(t.toString());
			// A:1.0 B D
			if (node.containsAdjacentNodes()) {
				// 计算前的数据
				sourceNode = node;
			} else {
				// B:0.5 D:0.5
				sum = sum + node.getPageRank();
			}
		}
		// 计算PR值4.0为页面总数
		double newPR = (0.15 / 4.0) + (0.85 * sum);
		System.out.println("***********new pageRank value is" + newPR);
		// 把新的PR值和之前的PR值进行比较
		double d = newPR - sourceNode.getPageRank();
		// j可正可负，加绝对值
		int j = (int) (d * 1000.0);
		j = Math.abs(j);
		System.out.println("---------------");
		// 累加
		arg2.getCounter(MyCountEnum.my).increment(j);
		sourceNode.setPageRank(newPR);
		arg2.write(arg0, new Text(sourceNode.toString()));

	}

}
