package com.asiainfo.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.asiainfo.Main.HbaseMainOld;
import com.asiainfo.Main.HbaseMain;

public class Log4JUtil {
	public static String LOGGER4JFILE = "log4j.properties";
	private static List<Long> timeList = new ArrayList<Long>();
	
	private static class SingletonLog4JUtil{
		private static Logger logger = Logger.getLogger(Thread.currentThread().getStackTrace()[3].getClassName());
	}

	@Deprecated
	public static Logger getLogger(Class<?> className) {
		PropertyConfigurator.configure(System.getProperty("user.dir")+"/conf/" + LOGGER4JFILE);
		return  SingletonLog4JUtil.logger;
	}
	
	public static Logger getLogger() {
		PropertyConfigurator.configure(System.getProperty("user.dir")+"/conf/" + LOGGER4JFILE);
		return  SingletonLog4JUtil.logger;
	}
	
	@Deprecated
	public static void getTimeLog(){
		HbaseMainOld.timeList.clear();
		HbaseMainOld.timeList.add(System.currentTimeMillis());
		
		for(Entry<String, String> entry:HbaseMainOld.timeMap.entrySet()){
			if(entry.getKey().contains("10000")){
				System.out.println(entry.getKey()+""+entry.getValue());
			}
		}
		for(Entry<String, String> entry:HbaseMainOld.timeMap.entrySet()){
			if(entry.getKey().contains("ops")){
				System.out.println(entry.getKey()+""+entry.getValue());
			}
		}
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
		HbaseMainOld.timeMap.clear();
		
	}
	
	public static void addNowts(){
		timeList.add(System.currentTimeMillis());
	}
	
	public static Long getTimeInterval(){
		if(timeList.size()>=2){
			return 
				ParamUtil.LOGBATCHSIZE * (timeList.get(timeList.size()-1) - timeList.get(timeList.size()-2))
				/ ParamUtil.INPUTBATCHSIZE;
 		}else{
 			return -1L;
 		}

	}
	
	public static void reTimeList(){
		timeList.clear();
		addNowts();
	}
}
