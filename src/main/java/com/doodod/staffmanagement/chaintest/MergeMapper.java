package com.doodod.staffmanagement.chaintest;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MergeMapper extends MapReduceBase implements
		Mapper<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MERGE_MAP_OK,
	}
	public void map(Text key, BytesWritable value,
			OutputCollector<Text, BytesWritable> output, Reporter reporter)
			throws IOException {

		reporter.getCounter(JobCounter.MERGE_MAP_OK).increment(1);
		output.collect(key, value);
		
		
		
	}

}
