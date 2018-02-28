package com.doodod.staffmanagement.chaintest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.doodod.staffmanagement.common.Coordinates;
import com.doodod.staffmanagement.message.Company.Employee;
import com.doodod.staffmanagement.message.Company.Location;

public class MergeReducer extends MapReduceBase implements
		Reducer<Text, BytesWritable, Text, BytesWritable> {
	enum JobCounter {
		MERGE_REDUCE_OK,
		SAME_LOCATION
		
	}
	public void reduce(Text key, Iterator<BytesWritable> values,
			OutputCollector<Text, BytesWritable> output, Reporter reporter)
			throws IOException {
		//combine the employee list together
				Employee.Builder eb = Employee.newBuilder();
				Employee.Builder mergeemployee = Employee.newBuilder();
				int times = 0;
				mergeemployee.clear();
				while(values.hasNext()){
					BytesWritable val = values.next();
					eb.mergeFrom(val.getBytes(), 0, val.getLength());
				}
//				for(BytesWritable val : values){
//					eb.mergeFrom(val.getBytes(), 0, val.getLength());				
//				}
				
				
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
				
				
				reporter.getCounter(JobCounter.MERGE_REDUCE_OK).increment(1);
				BytesWritable res = new BytesWritable(mergeemployee.build().toByteArray());
				
				output.collect(key, res);
	}

}
