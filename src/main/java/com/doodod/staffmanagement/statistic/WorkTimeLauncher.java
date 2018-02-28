package com.doodod.staffmanagement.statistic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WorkTimeLauncher extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		//calculate work time
		GenericOptionsParser options=new GenericOptionsParser(getConf(), arg0);
		Configuration hadoopConf = options.getConfiguration();
		Job job = new Job(hadoopConf);
		job.setJarByClass(WorkTimeLauncher.class);
		job.setMapperClass(WorkTimeMapper.class);
//		job.setReducerClass(WorkTimerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		job.waitForCompletion(true);			
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new Configuration(), new WorkTimeLauncher(),
				args));
	}

}
