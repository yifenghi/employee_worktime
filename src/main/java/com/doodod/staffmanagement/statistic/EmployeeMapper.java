package com.doodod.staffmanagement.statistic;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.doodod.staffmanagement.message.Company;
import com.doodod.staffmanagement.message.Company.Location;
import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.common.Coordinates;
import com.doodod.staffmanagement.message.Company.Employee;
import com.google.protobuf.ByteString;

public class EmployeeMapper extends TableMapper<Text, BytesWritable> {

	enum JobCounter {
		X_NUM,
		Y_NUM,
		Z_NUM,
		RECORD_NUM,
		QUALIFIER_NUM_ERROR,
		MAC_KEY_ERROR,
		MAC_NOT_IN_BRAND_MAP,
		NOT_LANXIN_DATA
	}		
	private static HashSet<String> MAC_SET = new HashSet<String>();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//read mac list from file:data/mac_list
		Charset charSet = Charset.forName("UTF-8");
		String macPath = context.getConfiguration().get(Common.CONF_MAC_LIST);
		BufferedReader macReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(macPath), charSet));
		String line = "";
		while ((line = macReader.readLine()) != null) {		
			MAC_SET.add(line);
		}
		macReader.close();
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		byte[] macArr = key.get();
		//hbase key : 0~1bit is mac type and 2~18bit is mac
		String macType = getMacFromArr(macArr, 0, Common.AP_MAC_LENGTH);
		String phoneMac = getMacFromArr(macArr, Common.AP_MAC_LENGTH, macArr.length);
		Employee.Builder eb = Employee.newBuilder();
//		if (macType.equals(Common.TAG_MAC)&&MAC_SET.contains(phoneMac)) {
//			eb.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()));
//			eb.setUserType(Company.UserType.EMPLOYEE);
//		}
//		else {
//			context.getCounter(JobCounter.MAC_KEY_ERROR).increment(1);
//			return;
//		}
		if (macType.equals(Common.TAG_MAC)) {
			eb.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()));
			eb.setUserType(Company.UserType.EMPLOYEE);
		}else {
			context.getCounter(JobCounter.MAC_KEY_ERROR).increment(1);
			return;
		}
			
			
		List<Location.Builder> locationList = new ArrayList<Location.Builder>();//location list
		List<Long> timeStampList = new ArrayList<Long>();//timestamp list
		List<Long> clientTimeList = new ArrayList<Long>();//client time list
		List<Long> planarGraphList = new ArrayList<Long>();//floor id list
		List<Double> locationXList = new ArrayList<Double>();//location X list
		List<Double> locationYList = new ArrayList<Double>();//location Y list
		List<String> positionSysList = new ArrayList<String>();//position system name list
		
		//read data from hbase into the list 
		for (Cell cell : value.listCells()) {
			long timeStamp = cell.getTimestamp();
			if (!timeStampList.contains(timeStamp)) {
				timeStampList.add(timeStamp);
			}else{
				//do nothing
			}
			
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			if (Common.LOCATION_X.equals(qualifier)) {				
				double locationX = Bytes.toDouble(CellUtil.cloneValue(cell));
				locationXList.add(locationX);
				context.getCounter(JobCounter.X_NUM).increment(1);
			}
			else if (Common.LOCATION_Y.equals(qualifier)) {
				double locationY = Bytes.toDouble(CellUtil.cloneValue(cell));
				locationYList.add(locationY);
				context.getCounter(JobCounter.Y_NUM).increment(1);
			}
			else if (Common.LOCATION_Z.equals(qualifier)) {
				long planarGraph = Bytes.toLong(CellUtil.cloneValue(cell));
				///测试阶段使用
				
				if(planarGraph != Common.FLOORID){
					
					context.getCounter(JobCounter.NOT_LANXIN_DATA).increment(1);
					return;
					
				}
				
				
				///
				planarGraphList.add(planarGraph);
				context.getCounter(JobCounter.Z_NUM).increment(1);
			}
			else if (Common.LOCATION_TIME.equals(qualifier)) {
				long clientTime = Bytes.toLong(CellUtil.cloneValue(cell));
				clientTimeList.add(clientTime);
			}
			else if (Common.LOCATION_PRO.equals(qualifier)) {
				String positionSys = Bytes.toString(CellUtil.cloneValue(cell));
				positionSysList.add(positionSys);
			}
		}
		
		if (timeStampList.size() != planarGraphList.size() 
				|| timeStampList.size() != locationXList.size()
				|| timeStampList.size() != locationYList.size()) {
			context.getCounter(JobCounter.QUALIFIER_NUM_ERROR).increment(1);
			return;
		}
		//combine the same location timestamps together
		Coordinates last = new Coordinates(0, 0, 0);
		for (int i = timeStampList.size() -1 ; i >= 0; i--) {
			Coordinates current = new Coordinates(locationXList.get(i), 
					locationYList.get(i), planarGraphList.get(i));
			
			if (!current.equals(last)) {
				//if no the location,then add last
				Location.Builder lb = Location.newBuilder();
				lb.addTimeStamp(timeStampList.get(i));
				lb.setLocationX(locationXList.get(i));
				lb.setLocationY(locationYList.get(i));
				lb.setPlanarGraph(planarGraphList.get(i));
				lb.setPositionSys(
						ByteString.copyFrom(positionSysList.get(i).getBytes()));
				locationList.add(lb);
			}
			else {
				//if the location is the same one,then combine the timestamps together
				Location.Builder lb = locationList.get(locationList.size() - 1);				
				lb.addTimeStamp(timeStampList.get(i));
			}	
			
			last.set(current);		
		}
		//sort same location's timestamps from low to high
		ArrayList<Long> ary=new ArrayList<Long>();
		for (Location.Builder lb : locationList) {
			//get the timestamps from protobuffer and sort and set into the protobuffer
			for(int i=0;i<lb.getTimeStampList().size();i++){
				ary.add(lb.getTimeStamp(i));
			}
			Collections.sort(ary);
			lb.clearTimeStamp();
			lb.addAllTimeStamp(ary);						
			eb.addLocation(lb);
			ary.clear();
		}
				
		eb.setCompanyid(ByteString.copyFrom(Common.COMPANY_ID.getBytes()));
		context.getCounter(JobCounter.RECORD_NUM).increment(1);	
		context.write(new Text(phoneMac+Common.CTRL_A+Common.COMPANY_ID), new  BytesWritable(eb.build().toByteArray()));
		//write into HDFS,construction is <phoneMac&companyId,Employee>
	}

	
	private String getMacFromArr(byte[] arr, int start, int end) {
		StringBuffer mac = new StringBuffer();
		for (int i = start; i < end; i++) {
			mac.append(String.format(Common.MAC_FORMAT, arr[i]));
			mac.append(':');
		}
		mac.deleteCharAt(mac.length() - 1);
		return mac.toString();
	}
}
