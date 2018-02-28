package com.doodod.staffmanagement.statistic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.doodod.staffmanagement.common.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class IncorrectMapper extends Mapper<Text, Text, Text, Text> {

	enum JobCounter {

		DB_WRITE_OK, 
		TEXT,
		TEXT1,
		MAP
	}
	private static long TODAY_MIDNIGTH_TIME = 0;
	private static String mongoDbName;
	private static MongoClient mongoClient;
	
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		context.getCounter(JobCounter.TEXT).increment(1);
		
		
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
		String absenteeismCount = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_ABSENTEEISMCOUNT);		
		String incorrectCount = context.getConfiguration().get(
				Common.MONGO_COLLECTION_EMPLOYEE_INCORRECTCOUNT);
		
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		String today = context.getConfiguration().get(Common.EMPLOYEE_SYSTEM_TODAY);
		try {
			TODAY_MIDNIGTH_TIME = timeFormat.parse(today+ " " + "00:00:00").getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}//每天00:00:00
		BasicDBObject todayTime = new BasicDBObject();
		todayTime.put(employeeDate, TODAY_MIDNIGTH_TIME);
		
		BasicDBObject employeeinfo = new BasicDBObject();
		BasicDBObject doc = new BasicDBObject();
		BasicDBObject query = new BasicDBObject();
		
		DBCursor cur = employeeCollection.find(new BasicDBObject("date",new Long(TODAY_MIDNIGTH_TIME)));
		
		Iterator<DBObject> iterator = cur.iterator();  
		
	    while (iterator.hasNext()) {  
	    	DBObject obj = iterator.next();
	    	
	    	String mac = obj.get(employeeMac).toString();
	    	long date = Long.parseLong(obj.get(employeeDate).toString()); 
	    	int startTime = 0;
	    	if(obj.get(employeeStarttime) == null){
	    		startTime = 1;
	    	}
	    	 
	    	int late = Integer.parseInt(obj.get(lateCount).toString());
	    	int leaveEarly = Integer.parseInt(obj.get(leaveEarlyCount).toString());
	    	int absenteeism = 0;
	    	int incorrect = 0;
	    	
	    	context.getCounter(JobCounter.TEXT1).increment(1);
	    	absenteeism = calculateAbsenteeism(startTime);
	    	incorrect = late + leaveEarly + absenteeism;
	    	
	    	query.put(employeeMac, mac);
	    	query.put(employeeDate, date);
			employeeinfo.append(absenteeismCount, absenteeism)
							.append(incorrectCount, incorrect);
			doc.append(Common.MONGO_OPTION_SET, employeeinfo);
			employeeCollection.update(query, doc, true, false);

			context.getCounter(JobCounter.DB_WRITE_OK).increment(1);
	    }  
		
	    
	  
		
	    
		mongoClient.close();
		
		
	}
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(JobCounter.MAP).increment(1);
		context.write(new Text("1"), new Text("1"));
		
	}
	private int calculateAbsenteeism(int startTime) {
		//计算旷工次数，会在第二天开始计算
		int count = 0;

		if(startTime == 1){
			count = 1;
		}
		
		return count;
	}
}
