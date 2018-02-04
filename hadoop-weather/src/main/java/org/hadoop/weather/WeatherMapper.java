package org.hadoop.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Weather, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] strs = StringUtils.split(value.toString(), "\t");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTime(simpleDateFormat.parse(strs[0]));
			Weather weather = new Weather();
			weather.setYear(calendar.get(Calendar.YEAR));
			weather.setMouth(calendar.get(Calendar.MONTH + 1));
			weather.setDay(calendar.get(Calendar.DAY_OF_MONTH));
			int hot = Integer.parseInt(strs[1].substring(0, strs[1].lastIndexOf("c")));
			weather.setHot(hot);
			context.write(weather, new IntWritable(hot));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
