package com.doodod.staffmanagement.statistic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.doodod.staffmanagement.common.Common;

public class FilterReducer extends Reducer<Text, IntWritable, Text, Text> {
	enum JobCounter {

		READ_COUNT_OK,
		READ_COUNT_ERROR,
	}
	public static int count;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value:values){
			sum += value.get();
			
		}
		if(sum >= count){
			context.write(key,new Text());
		}

		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		try {
			count = Integer.parseInt(context.getConfiguration().get(Common.FILTER_COUNT));
			context.getCounter(JobCounter.READ_COUNT_OK);
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			context.getCounter(JobCounter.READ_COUNT_ERROR);
			e.printStackTrace();
		}
		
	}

	

}
