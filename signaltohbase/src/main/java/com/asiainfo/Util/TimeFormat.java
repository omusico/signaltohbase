package com.asiainfo.Util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeFormat {
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	public long Date2long(String time) {
		Date date = null;
		try {
			date = sdf.parse(time);
			date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date.getTime();
	}

	public String long2String(String time) {
		String fulltime = time + "000";
		long longtime = Long.valueOf(fulltime);
		Date datetime = new Date(longtime);
		return sdf.format(datetime);
	}

	public String long2String(long current) {
		Date datetime = new Date(current);
		return sdf.format(datetime);
	}
}
