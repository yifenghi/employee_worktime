package com.doodod.staffmanagement.statistic;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.message.Company.Employee;
import com.doodod.staffmanagement.message.Company.Location;
import com.doodod.staffmanagement.statistic.WorkTimerMapper_bak.JobCounter;

public class SeeCustomerDwellMapper extends
		Mapper<Text, BytesWritable, Text, Text> {

	@Override
	protected void map(Text key, BytesWritable value,Context context)
			throws IOException, InterruptedException {

		String arr[] = key.toString().split(Common.CTRL_A, -1);
		if (arr.length != 2) {
			context.getCounter(JobCounter.KEY_FORMAT_ERROR).increment(1);
			return;
		}
		String phoneMac = arr[0];
		String companyID = arr[1];
		Employee.Builder eb = Employee.newBuilder();
		eb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		String seeMac=context.getConfiguration().get(Common.SEE_MAC);
		
		if(phoneMac.equalsIgnoreCase(seeMac)){
			
			List<Long> timeList = new ArrayList<Long>();
			for(Location loc:eb.getLocationList()){
				timeList.addAll(loc.getTimeStampList());
				
			}
			Collections.sort(timeList);
//			StringBuffer sb = new StringBuffer();
//			sb.delete(0, sb.length());
//			if(eb.getLocationCount()>1){
//				String PositionSys = new String(eb.getLocation(0).getPositionSys().toByteArray());
//				sb.append(PositionSys);
//			}
//			
//			for(Long l:timeList){
//				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//				sb.append(sdf.format(l));
//				sb.append("\n");
//			}
//			context.write(new Text(phoneMac), new Text(sb.toString()));
			
			
			StringBuffer sb = new StringBuffer();
			sb.delete(0, sb.length());
			for(Location loc:eb.getLocationList()){
				sb.append("\n");
				sb.append(loc.getLocationX()).append("\t").append(loc.getLocationY());
			
			}
			context.write(new Text(phoneMac), new Text(sb.toString()));
			
			
		}else{
			return;
		}
		
	}

}
