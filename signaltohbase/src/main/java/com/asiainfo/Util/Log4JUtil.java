package com.asiainfo.Util;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Log4JUtil {
	private static final String LOGGER4JFILE = "log4j.properties";

	public static Logger getLogger(Class<?> className) {
		PropertyConfigurator.configure(System.getProperty("user.dir")+"/etc/" + LOGGER4JFILE);
		System.out.println("user.dir is " + System.getProperty("user.dir"));
		Logger logger = Logger.getLogger(className);
		return logger;
	}
}
