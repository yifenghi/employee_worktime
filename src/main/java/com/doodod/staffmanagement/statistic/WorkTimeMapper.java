package com.doodod.staffmanagement.statistic;
//add by lifeng
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.staffmanagement.common.Common;
import com.doodod.staffmanagement.common.TimeCluster;
import com.doodod.staffmanagement.message.Company.Employee;
import com.doodod.staffmanagement.message.Company.Location;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class WorkTimeMapper extends Mapper<Text, BytesWritable, Text, Text> {

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
		ERROR_DATA
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
	
	
	private static DB mongoDb;
	private static String mongoDbName;
	private static MongoClient mongoClient;
	
	//临时添加
//	private static Set<String> filterSet = new HashSet<String>();
	//
	
	

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
				dateMap.put(mac, TODAY_MIDNIGTH_TIME);
				startTimeMap.put(mac, null);
				endTimeMap.put(mac, null);
				workTimeMap.put(mac, null);
				overTimeMap.put(mac, null);
				
				
				lateMap.put(mac, 0);
				leaveEarlyMap.put(mac, 0);
				
				context.getCounter(JobCounter.READ_OK).increment(1);
			
			}
			
		//	mongoClient.close();
			
			
			
			
			
//			String filterPath = context.getConfiguration().get(
//					Common.FILTER_LIST);
//			BufferedReader filterReader = new BufferedReader(
//					new InputStreamReader(new FileInputStream(filterPath), charSet));
//			String line = "";		
//			while ((line = filterReader.readLine()) != null) {				
//
//				String[] ary = line.split("\t", -1);
//				filterSet.add(ary[0]);
//				context.getCounter(JobCounter.READ_FILTER_OK).increment(1);
//			}
//			filterReader.close();
			
			///
			

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
		//临时添加过滤mac
//		if(filterSet.contains(phoneMac.substring(0, 11))){
//			context.getCounter(JobCounter.FILTER_DATA).increment(1);
//			return;
//		}
		
		//
		
		
		
		
		///临时注释去掉过滤
		if (!dateMap.containsKey(phoneMac)) {
			//if the phonemac is not employee,then drop the data
			context.getCounter(JobCounter.PHONEMAC_ERROR).increment(1);
			return;
		} else {
//			dateMap.put(phoneMac, TODAY_MIDNIGTH_TIME);
			context.getCounter(JobCounter.TEXT1).increment(1);
		}

		
		
		
		Employee.Builder eb = Employee.newBuilder();
		eb.clear().mergeFrom(value.getBytes(), 0, value.getLength());
		long startTime = Common.MAX;
		long endTime = Common.MIN;
		long workTime = 0;
		long overTime = 0;
		
		int late = 0;
		int leaveEarly = 0;
		
		
		
		
		workTime = TimeCluster.getTimeDwell(eb.build(), Common.FILTER_TIME);
		
		
		///临时添加		
//		if(workTime <= 5*60*1000){
//			//临时添加过滤小于5分钟的人员
//			context.getCounter(JobCounter.LOW_5MIN).increment(1);
//			return; 
//		}else{
//			dateMap.put(phoneMac, TODAY_MIDNIGTH_TIME);
//		}
		//
		
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
		
		if(startTime != 0){
			
			late = calculateLate(startTime,BEFORE_WORK_TIME);
			
		}
		if(endTime != 0){
			
			leaveEarly = calculateLeaveEarly(endTime,AFTERNOON_START_WORK_TIME);
			
		}
		
		
		
		
/*		if (startTime < TODAY_MIDNIGTH_TIME && endTime >=TODAY_MIDNIGTH_TIME) {
			if (endTime <= BEFORE_WORK_TIME) {

				overTime = endTime - TODAY_MIDNIGTH_TIME;

			} else if (endTime > BEFORE_WORK_TIME
					&& endTime < OVERWORK_START_TIME) {

				overTime = BEFORE_WORK_TIME - TODAY_MIDNIGTH_TIME;

			} else if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = (BEFORE_WORK_TIME - TODAY_MIDNIGTH_TIME)
						+ (endTime - OVERWORK_START_TIME);
			}
		} else if (startTime >= TODAY_MIDNIGTH_TIME
				&& startTime <= BEFORE_WORK_TIME) {

			if (endTime <= BEFORE_WORK_TIME) {

				overTime = endTime - startTime;

			} else if (endTime > BEFORE_WORK_TIME
					&& endTime < OVERWORK_START_TIME) {

				overTime = BEFORE_WORK_TIME - startTime;

			} else if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = (BEFORE_WORK_TIME - startTime)
						+ (endTime - OVERWORK_START_TIME);
			}

		} else if (startTime > BEFORE_WORK_TIME
				&& startTime < MORNING_START_WORK_TIME) {

			if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = endTime - OVERWORK_START_TIME;
			}

		} else if (startTime > MORNING_START_WORK_TIME
				&& startTime < MORNING_END_WORK_TIME) {

			if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = endTime - OVERWORK_START_TIME;
			}

		} else if (startTime > MORNING_END_WORK_TIME
				&& startTime < AFTERNOON_START_WORK_TIME) {

			if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = endTime - OVERWORK_START_TIME;
			}

		} else if (startTime > AFTERNOON_START_WORK_TIME
				&& startTime < AFTERNOON_END_WORK_TIME) {

			if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = endTime - OVERWORK_START_TIME;
			}

		} else if (startTime > AFTERNOON_END_WORK_TIME
				&& startTime < OVERWORK_START_TIME) {

			if (endTime >= OVERWORK_START_TIME
					&& endTime <= TOMORROW_START_TIME) {

				overTime = endTime - OVERWORK_START_TIME;
			}

		} else if (startTime > OVERWORK_START_TIME
				&& startTime < TOMORROW_START_TIME) {

			if (endTime <= TOMORROW_START_TIME) {

				overTime = endTime - startTime;
			} else {
				overTime = TOMORROW_START_TIME - startTime;
			}

		}
*/
		
		
		if (startTimeMap.containsKey(phoneMac) && startTime != Common.MAX) {
			startTimeMap.put(phoneMac, startTime);

		}
		if (endTimeMap.containsKey(phoneMac) && endTime != Common.MIN) {
			endTimeMap.put(phoneMac, endTime);

		}
		if (workTimeMap.containsKey(phoneMac) && workTime != 0) {
			workTimeMap.put(phoneMac, workTime);

		}
		if (overTimeMap.containsKey(phoneMac) && overTime != 0) {
			overTimeMap.put(phoneMac, overTime);

		}
		
		
		if (lateMap.containsKey(phoneMac)) {
			lateMap.put(phoneMac, late);

		}
		if (leaveEarlyMap.containsKey(phoneMac)) {
			leaveEarlyMap.put(phoneMac, leaveEarly);

		}

		
//现在为demo后边需要把改回来
		
//		if (startTime != Common.MAX) {
//			startTimeMap.put(phoneMac, startTime);
//
//		}
//		if (endTime != Common.MIN) {
//			endTimeMap.put(phoneMac, endTime);
//
//		}
//		if (workTime != 0) {
//			workTimeMap.put(phoneMac, workTime);
//
//		}
//		if (overTime != 0) {
//			overTimeMap.put(phoneMac, overTime);
//
//		}
//		
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		context.write(new Text(phoneMac), new Text(sdf.format(startTime) + ","
				+ sdf.format(endTime) + "," + workTime / 60000 + "," + overTime
				/ 60000 +"," + late + "," + leaveEarly));
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

}

