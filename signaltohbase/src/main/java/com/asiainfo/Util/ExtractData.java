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
	public static final int LOC_ROAM_AREA = 32;
	public static final int LOC_LON = 35;
	public static final int LOC_LAT = 36;
	public static final int LOC_IMSI = 2;
	public static final int LOC_IMEI = 3;

	// public static final int VOC_MO_HOST_AREA = 37;
	public static final int VOC_MO_HOST_AREA = 46;
	public static final int VOC_MO_ROAM_AREA = 39;
	public static final int VOC_MO_LON = 42;
	public static final int VOC_MO_LAT = 43;
	public static final int VOC_MO_IMSI = 1;
	public static final int VOC_MO_IMEI = 2;

	// public static final int VOC_MT_HOST_AREA = 37;
	public static final int VOC_MT_HOST_AREA = 46;
	public static final int VOC_MT_ROAM_AREA = 39;
	public static final int VOC_MT_LON = 42;
	public static final int VOC_MT_LAT = 43;
	public static final int VOC_MT_IMSI = 4;
	public static final int VOC_MT_IMEI = 5;

	// public static final int SMS_MO_HOST_AREA = 21;
	public static final int SMS_MO_HOST_AREA = 32;
	public static final int SMS_MO_ROAM_AREA = 23;
	public static final int SMS_MO_LON = 26;
	public static final int SMS_MO_LAT = 27;
	public static final int SMS_MO_IMSI = 2;
	public static final int SMS_MO_IMEI = 3;

	// public static final int SMS_MT_HOST_AREA = 29;
	public static final int SMS_MT_HOST_AREA = 32;
	public static final int SMS_MT_ROAM_AREA = 23;
	public static final int SMS_MT_LON = 26;
	public static final int SMS_MT_LAT = 27;
	public static final int SMS_MT_IMSI = 5;
	public static final int SMS_MT_IMEI = 6;

	public static String extractData(String mdn, String signalType, int extraType, String lac, String ci, String enterTime, String endTime,
			String[] fields) {
		switch (extraType) {
		case LOC:
			return combinData(mdn, signalType, LOC_HOST_AREA,    LOC_ROAM_AREA,    lac, ci, LOC_LON,     LOC_LAT,   LOC_IMSI, 
					LOC_IMEI,    enterTime, endTime, fields);
		case VOC_MO:
			return combinData(mdn, signalType, VOC_MO_HOST_AREA, VOC_MO_ROAM_AREA, lac, ci, VOC_MO_LON, VOC_MO_LAT, VOC_MO_IMSI,
					VOC_MO_IMEI, enterTime, endTime, fields);
		case VOC_MT:
			return combinData(mdn, signalType, VOC_MT_HOST_AREA, VOC_MT_ROAM_AREA, lac, ci, VOC_MT_LON, VOC_MT_LAT, VOC_MT_IMSI,
					VOC_MT_IMEI, enterTime, endTime, fields);
		case SMS_MO:
			return combinData(mdn, signalType, SMS_MO_HOST_AREA, SMS_MO_ROAM_AREA, lac, ci, SMS_MO_LON, SMS_MO_LAT, SMS_MO_IMSI,
					SMS_MO_IMEI, enterTime, endTime, fields);
		case SMS_MT:
			return combinData(mdn, signalType, SMS_MT_HOST_AREA, SMS_MT_ROAM_AREA, lac, ci, SMS_MT_LON, SMS_MT_LAT, SMS_MT_IMSI,
					SMS_MT_IMEI, enterTime, endTime, fields);
		default:
			return null;
		}

	}

	public static String combinData(String mdn, String signalType, int idxHostArea, int idxRoamArea, String lac, String ci, int idxLon, int idxLat,
			int imsi, int imei, String enterTime, String endTime, String[] fields) {
		
		StringBuffer sb = new StringBuffer();
//	    sb.append(mdn          ).append(",")
//		  .append(signalType   ).append(",")
//		  .append(fields[idxHostArea]         ).append(",")
//		  .append(RoamAreaFormat(fields[idxRoamArea]        )).append(",")
//		  .append(lac     ).append(",")
//		  .append(ci  ).append(",")
//		  .append(fields[idxLon]          ).append(",")
//		  .append(fields[idxLat]           ).append(",")
//	      .append(fields[imsi]    ).append(",")
//		  .append(fields[imei]     ).append(",")
//		  .append(endTime         ).append(",")
//		  .append(endTime         ).append(",")
//		  .append("").append(",")
//		  .append(""     );
	    sb.append(fields[imsi]          ).append(",")
		  .append(signalType   ).append(",")
		  .append(fields[idxHostArea]         ).append(",")
		  .append(RoamAreaFormat(fields[idxRoamArea]        )).append(",")
		  .append(lac     ).append(",")
		  .append(ci  ).append(",")
		  .append(fields[idxLon]          ).append(",")
		  .append(fields[idxLat]           ).append(",")
	      .append(mdn    ).append(",")
		  .append(fields[imei]     ).append(",")
		  .append(endTime         ).append(",")
		  .append(endTime         ).append(",")
		  .append("").append(",")
		  .append(""     );
	    
	    String signalBean = new String(sb);

		return signalBean;
	}
	
	public static String bytesToString(byte[] bytes){
		if(bytes==null){
			return "";
		}else{
			return new String(bytes);
		}
	}
	
	public static String RoamAreaFormat(String city_id){
		if(!("").equals(city_id)){
			switch(Integer.valueOf(city_id)){
			case 12101:
				city_id = "571";
				break;
			case 12102:
				city_id = "574";
				break;
			case 12103:
				city_id = "577";
				break;
			case 12104:
				city_id = "573";
				break;
			case 12105:
				city_id = "572";
				break;
			case 12106:
				city_id = "575";
				break;
			case 12107:
				city_id = "579";
				break;
			case 12108:
				city_id = "570";
				break;
			case 12109:
				city_id = "580";
				break;
			case 12110:
				city_id = "576";
				break;
			case 12111:
				city_id = "578";
				break;
			default:
				break;	
			}
		}
		return city_id;
	}
}
