package com.doodod.staffmanagement.statistic;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;

import com.doodod.staffmanagement.common.Common;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class IncorrectStatistic {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		
		String mongoServerList = "10.1.8.45,10.1.8.46,10.1.8.47";
		String serverArr[] = mongoServerList.split(Common.COMMA, -1);
		if (serverArr.length != Common.MONGO_SERVER_NUM) {
			throw new RuntimeException("Get mongo server fail.");
		}
		String mongoServerFst = serverArr[0];
		String mongoServerSnd = serverArr[1];
		String mongoServerTrd = serverArr[2];
		String mongoDbName = "staffmanagement";
		int mongoServerPort = 27017;

		MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress(
				mongoServerFst, mongoServerPort), new ServerAddress(
				mongoServerSnd, mongoServerPort), new ServerAddress(
				mongoServerTrd, mongoServerPort)));		
		
		
		
		String employeeCollectionName = "lanxin_test";
		DB mongoDb = mongoClient.getDB(mongoDbName);
		DBCollection employeeCollection = mongoDb
				.getCollection(employeeCollectionName);

		String employeeDate = "date";
		String employeeMac = "mac";
		String employeeStarttime = "start_time";

		String lateCount = "late";
		String leaveEarlyCount = "leaveearly";
		String absenteeismCount = "absenteeism";		
		String incorrectCount = "incorrect";
		long TODAY_MIDNIGTH_TIME = 0;
		SimpleDateFormat timeFormat = new SimpleDateFormat(Common.TIME_FORMAT);
		String today = "2015-01-26";
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
	    	
	    	absenteeism = calculateAbsenteeism(startTime);
	    	incorrect = late + leaveEarly + absenteeism;
	    	
	    	query.put(employeeMac, mac);
	    	query.put(employeeDate, date);
			employeeinfo.append(absenteeismCount, absenteeism)
							.append(incorrectCount, incorrect);
			doc.append(Common.MONGO_OPTION_SET, employeeinfo);
			employeeCollection.update(query, doc, true, false);

			
	    }  
		
		mongoClient.close();

	}
	
	private static int calculateAbsenteeism(int startTime) {
		//计算旷工次数，会在第二天开始计算
		int count = 0;

		if(startTime == 1){
			count = 1;
		}
		
		return count;
	}

}
