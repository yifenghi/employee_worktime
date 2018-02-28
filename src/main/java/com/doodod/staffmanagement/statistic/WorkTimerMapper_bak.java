package com.doodod.staffmanagement.statistic;
//add by lifeng
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.common.Coordinates;
import com.doodod.staffmanagement.common.Polygon;
import com.doodod.staffmanagement.common.TimeCluster;
import com.doodod.staffmanagement.message.Company.AreaDwell;
import com.doodod.staffmanagement.message.Company.Employee;
import com.doodod.staffmanagement.message.Company.EmployeeInfo;
import com.doodod.staffmanagement.message.Company.Location;
import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class WorkTimerMapper_bak extends Mapper<Text, BytesWritable, Text, Text> {

	enum JobCounter {

		DB_WRITE_OK, 
		KEY_FORMAT_ERROR, 
		DATA_ERROR, 
		PHONEMAC_ERROR, 
		READ_OK, 		
		MONGO_READ_OK,
		TIME_DATA_ERROR,
		DB_TIME_NULL,
		TEXT1,
		TEXT2,
		MONGO_READ_OK2,
		MAP_OK,
		DATE_NULL,
		LOW_5MIN,
		READ_FILTER_OK,
		FILTER_DATA,
		ERROR_DATA,
		STORE_FROMAT_ERROR,
		FLOOR_ERROR,
		FRAME_FROMAT_ERROR,
		POINT_FROMAT_ERROR,
		POINT_LIST_NULL
	}

	private static long TODAY_MIDNIGTH_TIME = 0;
	private static long BEFORE_WORK_TIME = 0;
	private static long MORNING_START_WORK_TIME = 0;
	private static long MORNING_END_WORK_TIME = 0;
	private static long AFTERNOON_START_WORK_TIME = 0;
	private static long AFTERNOON_END_WORK_TIME = 0;
	private static long OVERWORK_START_TIME = 0;
	private static long TOMORROW_START_TIME = 0;
	//set up some Maps in order to write into the mongoDB,the key is phonemac
	private static Map<String, Long> dateMap = new HashMap<String, Long>();
	private static Map<String, Long> startTimeMap = new HashMap<String, Long>();
	private static Map<String, Long> endTimeMap = new HashMap<String, Long>();
	private static Map<String, Long> workTimeMap = new HashMap<String, Long>();
	private static Map<String, Long> overTimeMap = new HashMap<String, Long>();	
	private static Map<String,Integer> lateMap =new HashMap<String, Integer>();
	private static Map<String,Integer> leaveEarlyMap =new HashMap<String, Integer>();
	
	
	//set up area points list
//	private static Map<String, Long> areaDwellMap = new HashMap<String, Long>();
//	private static Map<String,Map<String, Long>> employee_area_dwell = new HashMap<String, Map<String,Long>>();
	
	private static Map<String, List<Coordinates>> areaFrameMap = new HashMap<String, List<Coordinates>>();
	//set up floor frame points list
	private static Map<String, List<Coordinates>> floorFrameMap = new HashMap<String, List<Coordinates>>();
	
	
	private static List<EmployeeInfo.Builder> employeeInfoList = new ArrayList<EmployeeInfo.Builder>();
	
	private static DB mongoDb;
	private static String mongoDbName;
	private static MongoClient mongoClient;

	
	

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		try {
			
			//read the company working time points from mongoDB
			String today = context.getConfiguration().get(Common.EMPLOYEE_SYSTEM_TODAY);
			TODAY_MIDNIGTH_TIME = timeFormat.parse(today+ " " + "00:00:00").getTime();//每天00:00:00			
			String mongoServerList = context.getConfiguration().get(
					Common.MONGO_SERVER_LIST);
			String serverArr[] = mongoServerList.split(Common.COMMA, -1);
			if (serverArr.length != Common.MONGO_SERVER_NUM) {
				throw new RuntimeException("Get mongo server fail.");
			}
			String mongoServerFst = serverArr[0];
			String mongoServerSnd = serverArr[1];
			String mongoServerTrd = serverArr[2];
			mongoDbName = context.getConfiguration().get(Common.MONGO_DB_NAME);
			int mongoServerPort = Integer.parseInt(context.getConfiguration()
					.get(Common.MONGO_SERVER_PORT));

			mongoClient = new MongoClient(Arrays.asList(new ServerAddress(
					mongoServerFst, mongoServerPort), new ServerAddress(
					mongoServerSnd, mongoServerPort), new ServerAddress(
					mongoServerTrd, mongoServerPort)));			
			
			String worktimeCollectionName = context.getConfiguration().get(
					Common.MONGO_COLLECTION_WORKTIME);
			mongoDb = mongoClient.getDB(mongoDbName);
			DBCollection worktimeCollection = mongoDb
					.getCollection(worktimeCollectionName);
			
			DBObject time=worktimeCollection.findOne();	//read one data from mongoDB
			//before work time point,such as 6:00:00
			BEFORE_WORK_TIME=timeFormat.parse(today+" "+time.get(Common.BEFORE_WORK_TIME).toString()).getTime();	
			//morning start work time ,such as 8:00:00
			MORNING_START_WORK_TIME=timeFormat.parse(today+" "+time.get(Common.MONING_START_WORK_TIME).toString()).getTime();
			//morning end work time point,such as 12:00:00
			MORNING_END_WORK_TIME=timeFormat.parse(today+" "+time.get(Common.MONING_END_WORK_TIME).toString()).getTime();
			//afternoon start work time point,such as 13:00:00
			AFTERNOON_START_WORK_TIME=timeFormat.parse(today+" "+time.get(Common.AFTERNOON_START_WORK_TIME).toString()).getTime();
			//afternoon end work time point,such as 18:00:00
			AFTERNOON_END_WORK_TIME=timeFormat.parse(today+" "+time.get(Common.AFTERNOON_END_WORK_TIME).toString()).getTime();
			//overwork start time point,such as 19:00:00
			OVERWORK_START_TIME=timeFormat.parse(today+" "+time.get(Common.OVERWORK_START_TIME).toString()).getTime();	
			String tommorrow=context.getConfiguration().get(Common.EMPLOYEE_SYSTEM_TOMORROW);
			TOMORROW_START_TIME = timeFormat.parse(tommorrow + " " + "00:00:00").getTime();//每天24:00:00
			
			/////
			
			String macCollectionName = context.getConfiguration().get(
					Common.MONGO_COLLECTION_MACINFO);
			//DB mongoDb = mongoClient.getDB(mongoDbName);
			DBCollection macCollection = mongoDb
					.getCollection(macCollectionName);			
			DBCursor cur = macCollection.find();
			DBObject macInfo = null;
			while(cur.hasNext()){
				macInfo = cur.next();
				String mac = macInfo.get("mac").toString().toLowerCase();
				dateMap.put(mac, null);
				EmployeeInfo.Builder employeeInfo = EmployeeInfo.newBuilder();
				employeeInfo.setPhoneMac(ByteString.copyFrom(mac.getBytes()));
				employeeInfoList.add(employeeInfo);
				context.getCounter(JobCounter.READ_OK).increment(1);
			
			}
	
			
			Charset charSet = Charset.forName("UTF-8");
			String shopPath = context.getConfiguration().get(Common.SHOP_CONF_STORE);
			String areaPath = context.getConfiguration().get(
					Common.CONF_AREA);
			BufferedReader areaReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(areaPath), charSet));
			BufferedReader shopReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(shopPath), charSet));			
			String line = "";		
			while ((line = areaReader.readLine()) != null) {				

				String[] ary = line.split(Common.CTRL_A, -1);
				if(ary.length !=2){
					continue;
				}
				String areaId = ary[0];
				String areaName = ary[1];
				for(EmployeeInfo.Builder employee : employeeInfoList){
					AreaDwell.Builder ab = AreaDwell.newBuilder();
					ab.setAreaName(ByteString.copyFrom(areaName.getBytes()));
					ab.setDwell(0);
					employee.addAreaDwell(ab);
				}
				
				while ((line = shopReader.readLine()) != null) {
					String[] arrBrand = line.split(Common.CTRL_A, -1);
					if (arrBrand.length != 6) {
						context.getCounter(JobCounter.STORE_FROMAT_ERROR).increment(1);		
						continue;
					}
					
					if(areaId.equalsIgnoreCase(arrBrand[3])){
						String coorList[] = arrBrand[5].split(Common.CTRL_B, -1);
						List<Coordinates> coordinatesList = new ArrayList<Coordinates>();
						for (int i = 0; i < coorList.length; i++) {
							String coorArr[] = coorList[i].split(Common.CTRL_C, -1);
							if (coorArr.length != 2) {								
								continue;
							}
							double x = Double.parseDouble(coorArr[0]);
							double y = Double.parseDouble(coorArr[1]);
							
							Coordinates coordinate = new Coordinates(x, y, 1);
							coordinatesList.add(coordinate);
						}
						
						if(coordinatesList.size() != 0){
							areaFrameMap.put(areaName, coordinatesList);
						}
												
					}
					
				}
				
//				context.getCounter(JobCounter.READ_FILTER_OK).increment(1);
			}
			areaReader.close();
			shopReader.close();
			String framePath = context.getConfiguration().get(Common.SHOP_CONF_FRAME);
			BufferedReader frameReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(framePath), charSet));
			line = "";
			while ((line = frameReader.readLine()) != null) {
				String[] arrFrame = line.split(Common.CTRL_A, -1);
				if (arrFrame.length != 5) {
					context.getCounter(JobCounter.FRAME_FROMAT_ERROR).increment(1);		
					continue;
				}
				String floor = arrFrame[0];
				//String shopInfo = arrFrame[0] + Common.CTRL_A + arrFrame[1];
				
				String coorList[] = arrFrame[4].split(Common.CTRL_B, -1);
				List<Coordinates> coordinatesList = new ArrayList<Coordinates>();
				for (int i = 0; i < coorList.length; i++) {
					String coorArr[] = coorList[i].split(Common.CTRL_C, -1);
					if (coorArr.length != 2) {
						context.getCounter(JobCounter.POINT_FROMAT_ERROR).increment(1);
						continue;
					}
					double x = Double.parseDouble(coorArr[0]);
					double y = Double.parseDouble(coorArr[1]);
					
					Coordinates coordinate = new Coordinates(x, y, 1);
					coordinatesList.add(coordinate);
				}
				
				if(coordinatesList.size() != 0){
					floorFrameMap.put(floor, coordinatesList);
				}
			
			}
			
			
			

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		
		context.getCounter(JobCounter.TEXT2).increment(1);
		
		String arr[] = key.toString().split(Common.CTRL_A, -1);
		if (arr.length != 2) {
			context.getCounter(JobCounter.KEY_FORMAT_ERROR).increment(1);
			return;
		}
		String phoneMac = arr[0];
		String companyID = arr[1];	
		Employee.Builder original = Employee.newBuilder();
		original.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		
		
		if(!floorFrameMap.containsKey(""+Common.FLOORID)){
			context.getCounter(JobCounter.FLOOR_ERROR).increment(1);
			return;
		}
		Employee.Builder eb = getAreaLocaions(original,floorFrameMap.get(""+Common.FLOORID));
		long startTime = Common.MAX;
		long endTime = Common.MIN;
		long workTime = -1;
		long overTime = -1;		
		int late = 0;
		int leaveEarly = 0;
		//calculate work time
		workTime = TimeCluster.getTimeDwell(eb.build(), Common.FILTER_TIME);

		
		for (Location.Builder lb : eb.getLocationBuilderList()) {

			if (startTime > lb.getTimeStamp(0)) {
				//search the minimum time as starttime from locations
				startTime = lb.getTimeStamp(0);
			}else{
				// do nothing
			}
			if (endTime < lb.getTimeStamp(lb.getTimeStampCount() - 1)) {
				//search the maximum time as endtime from locations
				endTime = lb.getTimeStamp(lb.getTimeStampCount() - 1);
			}else{
				// do nothing
			}
			
		}
		if (startTime > endTime) {
			context.getCounter(JobCounter.DATA_ERROR).increment(1);
			return;
		}
		//calculate overtime 
				
		overTime = calculateOverTime(eb.build(),Common.FILTER_TIME,TODAY_MIDNIGTH_TIME,BEFORE_WORK_TIME,
				OVERWORK_START_TIME,TOMORROW_START_TIME);
		if(startTime != Common.MAX){
			//calculate late count
			late = calculateLate(startTime,BEFORE_WORK_TIME);
			
		}
		if(endTime != Common.MIN){
			//calculate leave early count
			leaveEarly = calculateLeaveEarly(endTime,AFTERNOON_START_WORK_TIME);
			
		}
	
		
		for(EmployeeInfo.Builder employee : employeeInfoList){
			String mac = new String(employee.getPhoneMac().toByteArray());
			if(mac.equalsIgnoreCase(phoneMac)){
				employee.setDate(TODAY_MIDNIGTH_TIME);
				employee.setStartTime(startTime);
				employee.setEndTime(endTime);
				employee.setWorkTime(workTime);
				employee.setOverTime(overTime);
				employee.setLateCount(late);
				employee.setLeaveEarlyCount(leaveEarly);
				
				for(AreaDwell.Builder ab : employee.getAreaDwellBuilderList()){
					String areaName = new String(ab.getAreaName().toByteArray());
					if(!areaFrameMap.containsKey(areaName)){
						continue;
					}
					
					Employee.Builder e = getAreaLocaions(eb,areaFrameMap.get(areaName));
					long dwell = TimeCluster.getTimeDwell(e.build(), Common.FILTER_TIME);
					ab.setDwell(e.getLocationCount());
					//统计落在区域内的点
				}
				
				
			}
		}
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuffer sb = new StringBuffer();
		sb.setLength(0);
		
		for(EmployeeInfo.Builder employee : employeeInfoList){
			String mac = new String(employee.getPhoneMac().toByteArray());
			if(mac.equalsIgnoreCase(phoneMac)){
				for(AreaDwell.Builder ab : employee.getAreaDwellBuilderList()){
					sb.append(new String(ab.getAreaName().toByteArray()) + ":" + ab.getDwell()).append(",");
				}
			}
		}
		
		
		Employee.Builder e = getAreaLocaions(eb,areaFrameMap.get("meeting_room"));
		
		context.write(new Text(phoneMac), new Text(sdf.format(startTime) + ","
				+ sdf.format(endTime) + "," + workTime / 60000 + "," + overTime
				/ 60000 +"," + late + "," + leaveEarly + "," + "meeting_room:" + e.getLocationCount() + "remainder:" +original.getLocationCount() + "," + eb.getLocationCount()));
		
		
		
		context.getCounter(JobCounter.MAP_OK).increment(1);
	}



	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {


		//write data into mongoDB
		String employeeCollectionName = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE);
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection employeeCollection = mongoDb
				.getCollection(employeeCollectionName);

		String employeeDate = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_DATE);
		String employeeMac = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_MAC);
		String employeeStarttime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_STARTTIME);
		String employeeEndtime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_ENDTIME);
		String employeeWorktime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_WORKTIME);
		String employeeOvertime = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_OVERTIME);
		
		
		String businessStart = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_BUSINESSSTART);
		String businessEnd = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_BUSINESSEND);
		
		
		String lateCount = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_LATECOUNT);
		String leaveEarlyCount = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_LEAVEEARLYCOUNT);
		
		
		BasicDBObject query = new BasicDBObject();
		BasicDBObject employeeinfo = new BasicDBObject();
		BasicDBObject doc = new BasicDBObject();
		Iterator<String> iter = dateMap.keySet().iterator();
		while (iter.hasNext()) {
			String phoneMac = iter.next();
			query.put(employeeMac, phoneMac);
			if(dateMap.get(phoneMac) != null){
				query.put(employeeDate, dateMap.get(phoneMac));
			}else{
				context.getCounter(JobCounter.DATE_NULL).increment(1);				
				continue;
			}
			

			employeeinfo.append(employeeStarttime, startTimeMap.get(phoneMac))
					.append(employeeEndtime, endTimeMap.get(phoneMac))
					.append(employeeWorktime, workTimeMap.get(phoneMac))
					.append(employeeOvertime, overTimeMap.get(phoneMac))
					.append(businessStart, MORNING_START_WORK_TIME)
					.append(businessEnd, AFTERNOON_END_WORK_TIME)
					.append(lateCount, lateMap.get(phoneMac))
					.append(leaveEarlyCount, leaveEarlyMap.get(phoneMac));
			// .append(employeeIncorrect, startTimeMap.get(phoneMac)));
			doc.append(Common.MONGO_OPTION_SET, employeeinfo);
			employeeCollection.update(query, doc, true, false);

			context.getCounter(JobCounter.DB_WRITE_OK).increment(1);
		}

		mongoClient.close();
		
		
		
		
		StringBuffer sb = new StringBuffer();
		sb.setLength(0);
		Iterator<String> it = areaFrameMap.keySet().iterator();
		while(it.hasNext()){
			String n = it.next();
			sb.append(n).append(":");
			for(Coordinates c : areaFrameMap.get(n)){
				sb.append(c.getX()).append(",").append(c.getY());
			}
			sb.append("\n");
		}
		context.write(new Text("#####"), new Text(sb.toString()));

	}

	private long calculateOverTime(Employee customer, long timeSplit,long midStart,
			long midEnd,long nightStart,long nightEnd) {
		Employee.Builder eb = Employee.newBuilder();
		
		for(Location.Builder lb : customer.toBuilder().getLocationBuilderList()){			
			Location.Builder loc = Location.newBuilder();
			loc.setLocationX(lb.getLocationX());
			loc.setLocationY(lb.getLocationY());
			loc.setPlanarGraph(lb.getPlanarGraph());
			loc.setPositionSys(lb.getPositionSys());			
			for(long t : lb.getTimeStampList()){
				if(t >= midStart && t <= midEnd){
					loc.addTimeStamp(t);
				}
				if(t >= nightStart && t <= nightEnd){
					loc.addTimeStamp(t);
				}				
			}
			if(loc.getTimeStampList() != null&&loc.getTimeStampCount()!=0){
				
				eb.addLocation(loc);
				
			}else{
				
			}			
		}				
		long overTime = TimeCluster.getTimeDwell(eb.build(), timeSplit);
		return overTime;
	}
	
	private int calculateAbsenteeism(String phoneMac) {
		//计算旷工次数，会在第二天开始计算
		int count = 0;
		Date date = new Date();
		if(date.getTime() > TOMORROW_START_TIME - 1){
			if(startTimeMap.containsKey(phoneMac)){
				if(startTimeMap.get(phoneMac) != null){
					count = 1;
				}
			}
		}
		
		return count;
	}

	private int calculateLeaveEarly(long endTime,long time) {
		//计算早退次数
		int count = 0;
		if(endTime<time){
			//早退
			//如果当前时间小于下班时间，则不判断，用户今天的下班异常判断，下班时间一个半小时后开始计算早退次数
			Date date = new Date();
			if(date.getTime() > (endTime+(15*60*1000l))){
				count = 1;
			}
		}
		
		return count;
	}

	private int calculateLate(long startTime, long time) {
		//计算迟到次数
		
		if(startTime > time){
			return 1;
		}else{
			return 0;
		}
		
		
	}
	
	
	private Employee.Builder getAreaLocaions(Employee.Builder old,List<Coordinates> pointList){
		
//		Employee.Builder eb = old.clone();
//		Polygon poly = new Polygon(pointList);
//		
//		for(int i = 0; i < eb.getLocationCount(); i++){
//			if(!poly.coordinateInPolygon(new Coordinates(eb.getLocationBuilder(i).getLocationX(), eb.getLocationBuilder(i).getLocationY(), 1))){
//				eb.removeLocation(i);
//			}
//		}	
		Employee.Builder eb = Employee.newBuilder();
		Polygon poly = new Polygon(pointList);
		for(int i = 0; i < old.getLocationCount(); i++){
			if(poly.coordinateInPolygon(new Coordinates(old.getLocation(i).getLocationX(), old.getLocation(i).getLocationY(), 1))){
				eb.addLocation(old.getLocation(i));
			}
		}	
		
		
		return eb;
	}

}
