package com.doodod.staffmanagement.statistic;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.message.Company.Employee;
import com.doodod.staffmanagement.message.Mall.Customer;
import com.doodod.staffmanagement.message.Mall.Location;
import com.google.protobuf.ByteString;



public class EmployeeMapper1 extends Mapper<Text, BytesWritable, Text, BytesWritable> {

	enum JobCounter {
		NO_LOCATION,
		NOT_LANXIN_DATA,
		RECORD_NUM
	}	
	
	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {

		Customer.Builder cb = Customer.newBuilder();
		cb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		if(cb.getLocationCount() == 0){
			context.getCounter(JobCounter.NO_LOCATION).increment(1);
			return;
		}
		long planarGraph = cb.getLocation(0).getPlanarGraph();
		
		if(planarGraph != Common.FLOORID){
			
			context.getCounter(JobCounter.NOT_LANXIN_DATA).increment(1);
			return;
			
		}
		
		Employee.Builder eb = Employee.newBuilder();
		eb.setCompanyid(ByteString.copyFrom(Common.COMPANY_ID.getBytes()));
		eb.setPhoneMac(ByteString.copyFrom(key.copyBytes()));
		
		for(Location loc: cb.getLocationList()){
			
			com.doodod.staffmanagement.message.Company.Location.Builder lb = com.doodod.staffmanagement.message.Company.Location.newBuilder();
			lb.setLocationX(loc.getLocationX());
			lb.setLocationY(loc.getLocationY());
			lb.setPlanarGraph(loc.getPlanarGraph());
			lb.setPositionSys(loc.getPositionSys());
			lb.addAllTimeStamp(loc.getTimeStampList());
			eb.addLocation(lb.build());
			
		}
		
		context.getCounter(JobCounter.RECORD_NUM).increment(1);	
		context.write(new Text(key.toString()+Common.CTRL_A+Common.COMPANY_ID), new  BytesWritable(eb.build().toByteArray()));
		
		
	}

}
