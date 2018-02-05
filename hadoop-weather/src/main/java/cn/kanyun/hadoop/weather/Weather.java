package cn.kanyun.hadoop.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Weather implements WritableComparable<Weather> {
	private int year;
	private int mouth;
	private int day;
	// 温度
	private int hot;

	// 序列化/反序列化方法
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.year = arg0.readInt();
		this.mouth = arg0.readInt();
		this.day = arg0.readInt();
		this.hot = arg0.readInt();
	}

	// 序列化/反序列化方法
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(year);
		arg0.writeInt(mouth);
		arg0.writeInt(day);
		arg0.writeInt(hot);
	}

	// 比较
	public int compareTo(Weather o) {
		// TODO Auto-generated method stub
		int o1 = Integer.compare(this.year, o.getYear());
		if (o1 == 0) {
			int o2 = Integer.compare(this.mouth, o.getMouth());
			if (o2 == 0) {
				return Integer.compare(this.hot, o.getHot());
			}
			return o2;
		}

		return o1;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMouth() {
		return mouth;
	}

	public void setMouth(int mouth) {
		this.mouth = mouth;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

}
