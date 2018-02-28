package com.doodod.staffmanagement.statistic;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.doodod.staffmanagement.common.Common;



public class MergeMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

	enum JobCounter {
		MERGE_MAP_OK,
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(JobCounter.MERGE_MAP_OK).increment(1);
		context.write(key, value);
	}
}
