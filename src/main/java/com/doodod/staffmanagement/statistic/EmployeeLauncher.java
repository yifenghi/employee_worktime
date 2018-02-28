package com.doodod.staffmanagement.statistic;
//add by lifeng
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.doodod.staffmanagement.common.Common;


public class EmployeeLauncher extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
	
		GenericOptionsParser options = new GenericOptionsParser(getConf(), args);
		Configuration hadoopConf= options.getConfiguration();
		//scan 1hour ago hbase data
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		long timeStart = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_START)).getTime();
		//1 hour ago time
		long timeEnd = timeFormat.parse(
				hadoopConf.get(Common.BUSINESSTIME_NOW)).getTime();
		//now time
		Job job = new Job(hadoopConf);
		Scan scan = new Scan();
		scan.setCaching(3000);
		scan.setCacheBlocks(false);
		scan.setTimeRange(timeStart, timeEnd);
		scan.setMaxVersions();
		scan.addFamily(Bytes.toBytes(Common.TABLE_NAME));
		TableMapReduceUtil.initTableMapperJob(Common.TABLE_NAME, scan,
				EmployeeMapper.class, Text.class, BytesWritable.class, job);
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

		System.exit(ToolRunner.run(new Configuration(), new EmployeeLauncher(), args));

	}

}
