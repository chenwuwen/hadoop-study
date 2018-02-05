package cn.kanyun.hadoop.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroup extends WritableComparator {

	// 必须
	public WeatherGroup() {
		super(Weather.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		int o1 = Integer.compare(w1.getYear(), w2.getYear());
		if (o1 == 0) {
			return Integer.compare(w1.getMouth(), w2.getMouth());
		}
		return o1;
	}

}
