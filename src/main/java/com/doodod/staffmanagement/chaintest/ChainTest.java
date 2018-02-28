package com.doodod.staffmanagement.chaintest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ChainTest extends Configured implements Tool {

	
	public int run(String[] arg0) throws Exception {
		
		
		
		GenericOptionsParser options=new GenericOptionsParser(getConf(), arg0);
		Configuration hadoopConf = options.getConfiguration();
		JobConf jobConf = new JobConf(hadoopConf,ChainTest.class);
		
		
		ChainMapper.addMapper(jobConf, EmployeeMapper1.class, Text.class, BytesWritable.class,
				Text.class, BytesWritable.class, true, new JobConf(false));
		ChainMapper.addMapper(jobConf, MergeMapper.class, Text.class, BytesWritable.class, 
				Text.class, BytesWritable.class, true, new JobConf(false));
		ChainReducer.setReducer(jobConf, MergeReducer.class, Text.class, BytesWritable.class,
				Text.class, BytesWritable.class,true, new JobConf(false));
		ChainMapper.addMapper(jobConf, MergeMapper.class, Text.class, BytesWritable.class, 
				Text.class, BytesWritable.class, true, new JobConf(false));	
		
//		ChainMapper.addMapper(jobConf, OutMapper.class, Text.class, BytesWritable.class, 
//				Text.class, Text.class, true, new JobConf(false));
//		Job job = new Job(jobConf);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(BytesWritable.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
//		job.waitForCompletion(true);
		
//		jobConf.setOutputKeyClass(Text.class);
//		jobConf.setOutputValueClass(Text.class);
		
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);
		
		JobClient.runJob(jobConf);
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		System.exit(ToolRunner.run(new Configuration(), new ChainTest(), args));
		
	}

}
