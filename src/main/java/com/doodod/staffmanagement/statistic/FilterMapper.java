package com.doodod.staffmanagement.statistic;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.statistic.WorkTimerMapper_bak.JobCounter;

public class FilterMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {

	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {
		String arr[] = key.toString().split(Common.CTRL_A, -1);
		if (arr.length != 2) {
			context.getCounter(JobCounter.KEY_FORMAT_ERROR).increment(1);
			return;
		}
		String phoneMac = arr[0];
		String mac = phoneMac.substring(0, 11);
		context.write(new Text(mac), new IntWritable(1));
		
	}

}
