package com.doodod.staffmanagement.statistic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.staffmanagement.common.Common;


public class MergeLauncher extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		//combine 1hour data into 1day
		GenericOptionsParser options = new GenericOptionsParser(getConf(), arg0);
		Configuration hadoopConf = options.getConfiguration();
		Job job = new Job(hadoopConf);
		job.setJarByClass(MergeLauncher.class);
		job.setReducerClass(MergeReducer.class);
		MultipleInputs.addInputPath(job,
				new Path(hadoopConf.get(Common.MERGE_INPUT_PART)),
				SequenceFileInputFormat.class, MergeMapper.class);
		MultipleInputs.addInputPath(job,
				new Path(hadoopConf.get(Common.MERGE_INPUT_TOTAL)),
				SequenceFileInputFormat.class, MergeMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		job.waitForCompletion(true);		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		System.exit(ToolRunner.run(new Configuration(), new MergeLauncher(), args));

	}

}
