package cn.kanyun.hadoop.commend.step2;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CommendSort extends WritableComparator {

	public CommendSort() {
		// TODO Auto-generated constructor stub
		super(Commend.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Commend commend1 = (Commend) a;
		Commend commend2 = (Commend) b;
		int c = commend1.getName1().compareTo(commend2.getName1());
		if (c == 0) {
			// 亲密度降序
			return -Integer.compare(commend1.getHot(), commend2.getHot());
		}
		return c;
	}

}
