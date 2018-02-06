package cn.kanyun.hadoop.commend.step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.OL;

public class Commend implements WritableComparable<Commend> {

	private String name1;
	private String name2;
	// 亲密度
	private int hot;

	public String getName1() {
		return name1;
	}

	public void setName1(String name1) {
		this.name1 = name1;
	}

	public String getName2() {
		return name2;
	}

	public void setName2(String name2) {
		this.name2 = name2;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		// readUTF()表示字符串
		this.name1 = arg0.readUTF();
		this.name2 = arg0.readUTF();
		this.hot = arg0.readInt();

	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(name1);
		arg0.writeUTF(name2);
		arg0.writeInt(hot);

	}

	public int compareTo(Commend o) {
		// TODO Auto-generated method stub
		int c = this.name1.compareTo(o.getName1());
		if (c==0) {
			return Integer.compare(this.hot, o.getHot());
		}
		return c;
	}

}
