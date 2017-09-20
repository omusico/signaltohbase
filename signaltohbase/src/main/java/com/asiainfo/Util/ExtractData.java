package com.asiainfo.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractData {
	public static final Logger logger = LoggerFactory.getLogger(ExtractData.class);
	// 数据类型
	public static final int LOC = 0;
	public static final int VOC_MO = 1;
	public static final int VOC_MT = 2;
	public static final int SMS_MO = 3;
	public static final int SMS_MT = 4;
	public static final int PS = 5;

	// public static final int LOC_HOST_AREA = 16;
	public static final int LOC_HOST_AREA = 47;
	public static final int LOC_ROAM_AREA = 33;
	public static final int LOC_LON = 35;
	public static final int LOC_LAT = 36;
	public static final int LOC_IMSI = 2;
	public static final int LOC_IMEI = 3;

	// public static final int VOC_MO_HOST_AREA = 37;
	public static final int VOC_MO_HOST_AREA = 46;
	public static final int VOC_MO_ROAM_AREA = 40;
	public static final int VOC_MO_LON = 42;
	public static final int VOC_MO_LAT = 43;
	public static final int VOC_MO_IMSI = 1;
	public static final int VOC_MO_IMEI = 2;

	// public static final int VOC_MT_HOST_AREA = 37;
	public static final int VOC_MT_HOST_AREA = 46;
	public static final int VOC_MT_ROAM_AREA = 40;
	public static final int VOC_MT_LON = 42;
	public static final int VOC_MT_LAT = 43;
	public static final int VOC_MT_IMSI = 4;
	public static final int VOC_MT_IMEI = 5;

	// public static final int SMS_MO_HOST_AREA = 21;
	public static final int SMS_MO_HOST_AREA = 32;
	public static final int SMS_MO_ROAM_AREA = 24;
	public static final int SMS_MO_LON = 26;
	public static final int SMS_MO_LAT = 27;
	public static final int SMS_MO_IMSI = 2;
	public static final int SMS_MO_IMEI = 3;

	// public static final int SMS_MT_HOST_AREA = 29;
	public static final int SMS_MT_HOST_AREA = 32;
	public static final int SMS_MT_ROAM_AREA = 24;
	public static final int SMS_MT_LON = 26;
	public static final int SMS_MT_LAT = 27;
	public static final int SMS_MT_IMSI = 5;
	public static final int SMS_MT_IMEI = 6;

	public static String extractData(int extraType, String lac, String ci, String enterTime, String endTime,
			String[] fields) {
		// 0 1 2 3 4 5 6 7 8 9
		// 用户归属地 漫游地 LAC CI 经度 纬度 IMSI IMEI 进入当前CI时间 最后出现时间点
		switch (extraType) {
		case LOC:
			return combinData(LOC_HOST_AREA, LOC_ROAM_AREA, lac, ci, LOC_LON, LOC_LAT, LOC_IMSI, LOC_IMEI, enterTime,
					endTime, fields);
		case VOC_MO:
			return combinData(VOC_MO_HOST_AREA, VOC_MO_ROAM_AREA, lac, ci, VOC_MO_LON, VOC_MO_LAT, VOC_MO_IMSI,
					VOC_MO_IMEI, enterTime, endTime, fields);
		case VOC_MT:
			return combinData(VOC_MT_HOST_AREA, VOC_MT_ROAM_AREA, lac, ci, VOC_MT_LON, VOC_MT_LAT, VOC_MT_IMSI,
					VOC_MT_IMEI, enterTime, endTime, fields);
		case SMS_MO:
			return combinData(SMS_MO_HOST_AREA, SMS_MO_ROAM_AREA, lac, ci, SMS_MO_LON, SMS_MO_LAT, SMS_MO_IMSI,
					SMS_MO_IMEI, enterTime, endTime, fields);
		case SMS_MT:
			return combinData(SMS_MT_HOST_AREA, SMS_MT_ROAM_AREA, lac, ci, SMS_MT_LON, SMS_MT_LAT, SMS_MT_IMSI,
					SMS_MT_IMEI, enterTime, endTime, fields);
		default:
			return "";
		}

	}

	public static String combinData(int idxHostArea, int idxRoamArea, String lac, String ci, int idxLon, int idxLat,
			int imsi, int imei, String enterTime, String endTime, String[] fields) {
		StringBuilder sb = new StringBuilder();
		sb.append(fields[idxHostArea]).append(",").append(fields[idxRoamArea]).append(",").append(lac).append(",")
				.append(ci).append(",").append(fields[idxLon]).append(",").append(fields[idxLat]).append(",")
				.append(fields[imsi]).append(",").append(fields[imei]).append(",").append(endTime).append(",")
				.append(endTime);
		return sb.toString();
	}
}
