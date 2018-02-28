package com.doodod.staffmanagement.chaintest;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.message.Company.Employee;

public class OutMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text> {

	public void map(Text key, BytesWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String arr[] = key.toString().split(Common.CTRL_A, -1);
		if (arr.length != 2) {
			
			return;
		}
		String phoneMac = arr[0];
		String companyID = arr[1];
		Employee.Builder eb = Employee.newBuilder();
		eb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		output.collect(new Text(phoneMac), new Text(""+eb.getLocationCount()));
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		super.close();
	}

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);
	}

}
