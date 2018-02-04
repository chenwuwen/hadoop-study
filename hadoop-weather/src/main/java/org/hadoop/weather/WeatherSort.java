package org.hadoop.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSort extends WritableComparator {

	// 必须
	public WeatherSort() {
		// TODO Auto-generated constructor stub
		super(Weather.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		int o1 = Integer.compare(w1.getYear(), w2.getYear());
		if (o1 == 0) {
			int o2 = Integer.compare(w1.getMouth(), w2.getMouth());
			if (o2 == 0) {
				return -Integer.compare(w1.getHot(), w2.getHot());
			}
			return o2;
		}

		return o1;
	}

}
