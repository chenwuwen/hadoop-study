package cn.kanyun.hadoop.commend.step2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CommendGroup extends WritableComparator{
	public CommendGroup() {
		// TODO Auto-generated constructor stub
		super(Commend.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Commend commend1 = (Commend) a;
		Commend commend2 = (Commend) b;
		int c = commend1.getName1().compareTo(commend2.getName1());
		return c;
	}
}
