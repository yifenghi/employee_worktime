package com.doodod.staffmanagement.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import com.doodod.staffmanagement.common.Coordinates;
import com.doodod.staffmanagement.message.Company.Employee;
import com.doodod.staffmanagement.message.Company.Location;
import com.google.protobuf.ByteString;

public class MergeReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MERGE_REDUCE_OK,
		SAME_LOCATION
		
	}
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,Context context)
			throws IOException, InterruptedException {
		//combine the employee list together
		Employee.Builder eb = Employee.newBuilder();
		Employee.Builder mergeemployee = Employee.newBuilder();
		int times = 0;
		mergeemployee.clear();
		for(BytesWritable val : values){
			eb.mergeFrom(val.getBytes(), 0, val.getLength());				
		}
		
		
		mergeemployee.setCompanyid(eb.getCompanyid());
		mergeemployee.setPhoneMac(eb.getPhoneMac());
		mergeemployee.setUserType(eb.getUserType());
				
		
		Map<Coordinates,Location.Builder> locationMap=new HashMap<Coordinates, Location.Builder>();
		
		
		for(Location.Builder loc:eb.getLocationBuilderList()){
			Coordinates current;
			current = new Coordinates(loc.getLocationX(), loc.getLocationY(), loc.getPlanarGraph());
			if(locationMap.containsKey(current)){	
				//if the location is the same one,then combine the timestamps together
				for(long ts:loc.getTimeStampList()){
					locationMap.get(current).addTimeStamp(ts);
				}
				
			}else{
				locationMap.put(current, loc);
			}
			
		}
		//put Locations into mergeemployee
		Iterator<Coordinates> it = locationMap.keySet().iterator();				
		while (it.hasNext()) {
			Coordinates coo = it.next();
			Location.Builder lcb = locationMap.get(coo);
			mergeemployee.addLocation(lcb);
		}	
			
		//sort same location's timestamps from low to high
		ArrayList<Long> ary=new ArrayList<Long>();
		for (Location.Builder lb : mergeemployee.getLocationBuilderList()) {
			//get the timestamps from protobuffer and sort and set into the protobuffer
			ary.clear();
			for(int i=0;i<lb.getTimeStampList().size();i++){
				ary.add(lb.getTimeStamp(i));
			}
			
			Collections.sort(ary);
			lb.clearTimeStamp();
			lb.addAllTimeStamp(ary);	
			
		}
		
		
		context.getCounter(JobCounter.MERGE_REDUCE_OK).increment(1);
		BytesWritable res = new BytesWritable(mergeemployee.build().toByteArray());
		
		context.write(key, res);
	}

}
