package com.asiainfo.Util;

import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.asiainfo.Main.HbaseMain;

public class Log4JUtil {
	private static final String LOGGER4JFILE = "log4j.properties";

	public static Logger getLogger(Class<?> className) {
		PropertyConfigurator.configure(System.getProperty("user.dir")+"/etc/" + LOGGER4JFILE);
		System.out.println("user.dir is " + System.getProperty("user.dir"));
		Logger logger = Logger.getLogger(className);
		return logger;
	}
	
	public static void getTimeLog(){
		HbaseMain.timeList.clear();
		HbaseMain.timeList.add(System.currentTimeMillis());
		
		for(Entry<String, String> entry:HbaseMain.timeMap.entrySet()){
			if(entry.getKey().contains("10000")){
				System.out.println(entry.getKey()+""+entry.getValue());
			}
		}
		for(Entry<String, String> entry:HbaseMain.timeMap.entrySet()){
			if(entry.getKey().contains("ops")){
				System.out.println(entry.getKey()+""+entry.getValue());
			}
		}
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
		HbaseMain.timeMap.clear();
		
	}
}
