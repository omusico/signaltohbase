package com.asiainfo.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.HbaseDao.HbaseInput;

public class DataFormatbak {
	private ReentrantLock lock = new ReentrantLock();
	public ArrayList<Get> getList = new ArrayList<Get>();



	// [start]constant
	/**
	 * 手机号码位置
	 */
	public final int LOC_MDN = 1;
	public final int VOC_MO_MDN = 3;
	public final int VOC_MT_MDN = 6;
	public final int SMS_MO_MDN = 4;
	public final int SMS_MT_MDN = 7;
	/**
	 * lac 和 ci 位置
	 */
	public final int LOC_START_LAC = 8;
	public final int LOC_START_CI = 9;
	public final int LOC_END_LAC = 10;
	public final int LOC_END_CI = 11;

	public final int VOC_START_LAC = 12;
	public final int VOC_START_CI = 13;
	public final int VOC_END_LAC = 14;
	public final int VOC_END_CI = 15;

	public final int SMS_START_LAC = 12;
	public final int SMS_START_CI = 13;

	/**
	 * imsi号码
	 */
	public final int LOC_IMSI = 2;
	public final int VOC_MO_IMSI = 1;
	public final int VOC_MT_IMSI = 4;
	public final int SMS_MO_IMSI = 2;
	public final int SMS_MT_IMSI = 5;

	/**
	 * 主叫被叫
	 */
	public final int VOC_CALLTYPE = 8;
	public final int SMS_TYPE = 9;

	/**
	 * 时间，位置和短信只需要开始时间，使用结束时间判断当前记录时间是否大于入库记录的时间
	 */
	public final int LOC_START_TIME = 0;
	public final int LOC_END_TIME = 14;
	public final int VOC_START_TIME = 0;
	public final int VOC_END_TIME = 26;
	public final int SMS_START_TIME = 0;
	public final int SMS_END_TIME = 19;

	public static final int IDX_LAC = 2;
	public static final int IDX_CI = 3;
	public static final int IDX_LASTTIME = 7;

	/**
	 * 呼叫类型 主叫
	 */
	public static final int MO = 0;
	/**
	 * 呼叫类型 被叫
	 */
	public static final int MT = 1;
	/**
	 * 呼叫类型 紧急呼叫
	 */
	public static final int EMERGENT_CALL = 3;
	/**
	 * 呼叫类型 MO切入呼叫
	 */
	public static final int MOHO = 5;
	/**
	 * 呼叫类型 MT切入呼叫
	 */
	public static final int MTHO = 6;

	/**
	 * 业务类型 切换/重定位
	 */
	public static final int HANDOVER_RELOCATION = 5;

	/**
	 * 业务类型 主叫短信
	 */
	public static final int MO_SMS = 0;
	/**
	 * 短信提交报告
	 */
	public static final int STATUS_REPORT_SMS = 4;
	/**
	 * 业务类型 被叫短信
	 */
	public static final int MT_SMS = 1;
	
	/**
	 * PS域信令漫游记录
	 */
	public static final int PS_ROAM_MSIMDN = 0;
	public static final int PS_ROAM_HOME_AREA_ID = 12;
	public static final int PS_ROAM_ROAM_AREA_ID = 13;
	public static final int PS_ROAM_LAC = 9;
	public static final int PS_ROAM_CI = 10;
	public static final int PS_ROAM_BTS_LON = 15;
	public static final int PS_ROAM_BTS_LAT = 16;
	public static final int PS_ROAM_IMSI = 1;
	public static final int PS_ROAM_IMEI = 2;
	public static final int PS_ROAM_TRANS_REQ_TIME_SEC = 8;
	
	// [end]

	
	public String PS_roam_insert(String message){
		StringBuffer PSsb = new StringBuffer();

		String[] fields = message.split("\\^", -1);
		if ((fields[PS_ROAM_MSIMDN] != null) && (!("").equals(fields[PS_ROAM_MSIMDN]))) {
			String UpdateTime = new TimeFormat().long2String(fields[PS_ROAM_TRANS_REQ_TIME_SEC]);

			PSsb.append(fields[PS_ROAM_MSIMDN]).append(",")
			    .append(fields[PS_ROAM_HOME_AREA_ID]).append(",")
			    .append(fields[PS_ROAM_ROAM_AREA_ID]).append(",")
				.append(fields[PS_ROAM_LAC]).append(",")
				.append(fields[PS_ROAM_CI]).append(",")
				.append(fields[PS_ROAM_BTS_LON]).append(",")
				.append(fields[PS_ROAM_BTS_LAT]).append(",")
				.append(fields[PS_ROAM_IMSI]).append(",")
				.append(fields[PS_ROAM_IMEI]).append(",")
				.append(UpdateTime).append(",")
				.append(UpdateTime);
			
			return new String(PSsb);
		}
		return "";
	}
	
	public String PS_insert(String message) {
		StringBuffer PSsb = new StringBuffer();

		String[] fields = message.split("\\^", -1);
		if ((fields[0] != null) && (!("").equals(fields[0]))) {
			String UpdateTime = new TimeFormat().long2String(fields[8]);

			PSsb.append(fields[0]).append(",")
			    .append(fields[12]).append(",")
			    .append(fields[14]).append(",")
				.append(fields[9]).append(",")
				.append(fields[10]).append(",")
				.append(fields[15]).append(",")
				.append(fields[16]).append(",")
				.append(fields[1]).append(",")
				.append(fields[2]).append(",")
				.append(UpdateTime).append(",")
				.append(UpdateTime);
			
			return new String(PSsb);
		}
		return "";
	}

	/**
	 * 实时位置数据
	 * 
	 * @param signalType
	 *            信令类型
	 * @param signalData
	 *            信令数据
	 */
	public String CS_insert(int signalType, String signalData) {
		lock.lock();
		try {
			String[] fields = signalData.split(",", -1);
			switch (signalType) {
			case Constants.LOCSIGNAL:
				return handleLoc(signalType, fields);
			case Constants.VOCSIGNAL:
				return handleVoc(signalType, fields);
			case Constants.SMSSIGNAL:
				return handleSms(signalType, fields);
			default:
				return "";
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 处理位置更新信令
	 * 
	 * @param signalType
	 *            信令类型
	 * @param fields
	 *            信令数据
	 */
	private String handleLoc(int signalType, String fields[]) {
		String enterTime = fields[LOC_START_TIME].trim();
		String mdn = fields[LOC_MDN].trim();
		String lac = fields[LOC_START_LAC].trim();
		String ci = fields[LOC_START_CI].trim();

		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			ci = fields[LOC_END_CI].trim();
			if ("65535".equals(ci) || "".equals(ci)) {
				return "";
			}

			lac = fields[LOC_END_LAC].trim();
			enterTime = fields[LOC_END_TIME].trim();
		}

		if (mdn.equals("")) {
			return "";
		}

		String endTime = fields[LOC_END_TIME].trim();

		return mdn + "," + ExtractData.extractData(ExtractData.LOC, lac, ci, enterTime, endTime, fields);
	}

	/**
	 * 处理语音信令
	 * 
	 * @param signalType
	 *            信令类型
	 * @param fields
	 *            信令数据
	 */
	private String handleVoc(int signalType, String fields[]) {
		String enterTime = fields[VOC_START_TIME].trim();
		String mdn = "";
		String lac = fields[VOC_START_LAC].trim();
		String ci = fields[VOC_START_CI].trim();
		Integer callType = Integer.parseInt(fields[VOC_CALLTYPE].trim());

		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			ci = fields[VOC_END_CI].trim();
			if ("65535".equals(ci) || "".equals(ci)) {
				return "";
			}

			lac = fields[VOC_END_LAC].trim();
			enterTime = fields[VOC_END_TIME].trim();
		}

		int extraType = ExtractData.VOC_MO;

		// 如果是主叫
		if (callType == MO || callType == MOHO || callType == EMERGENT_CALL) {
			mdn = fields[VOC_MO_MDN].trim();
		} else if (callType == MT || callType == MTHO) {
			// 如果是被叫
			mdn = fields[VOC_MT_MDN].trim();
			extraType = ExtractData.VOC_MT;
		}

		if (mdn.equals("")) {
			return "";
		}

		String endTime = fields[VOC_END_TIME].trim();

		return mdn + "," + ExtractData.extractData(extraType, lac, ci, enterTime, endTime, fields);
	}

	/**
	 * 处理短信信令
	 * 
	 * @param signalType
	 *            信令类型
	 * @param fields
	 *            信令数据
	 */
	private String handleSms(int signalType, String fields[]) {
		String enterTime = fields[SMS_START_TIME].trim();
		String mdn = "";
		String lac = fields[SMS_START_LAC].trim();
		String ci = fields[SMS_START_CI].trim();
		Integer smsType = Integer.parseInt(fields[SMS_TYPE].trim());

		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			return "";
		}

		int extraType = ExtractData.SMS_MO;

		// 如果是主叫
		if (smsType == MO_SMS || smsType == STATUS_REPORT_SMS) {
			mdn = fields[SMS_MO_MDN].trim();
		} else if (smsType == MT_SMS) {
			// 如果是被叫
			mdn = fields[SMS_MT_MDN].trim();
			extraType = ExtractData.SMS_MT;
		} else {
			return "";
		}

		if (mdn.equals("")) {
			return "";
		}

		String endTime = fields[SMS_END_TIME].trim();

		return  mdn + "," + ExtractData.extractData(extraType, lac, ci, enterTime, endTime, fields);
	}

//	public UpDelTrans compareData(String data, String result ,String TableName) {
//		UpDelTrans upDelTrans = new UpDelTrans();
//
//		String[] datas = data.split(",", -1);
//		String[] results = result.split(",", -1);	
//		if (new TimeFormat().Date2long(datas[10]) > new TimeFormat().Date2long(results[9])) {
//			if (!(datas[3].equals(results[2]) && datas[4].equals(results[3]))) {
//				List<String[]> DelArrayList = new ArrayList<String[]>();
//				if(upDelTrans.getDelArrayList()!=null){
//					DelArrayList = upDelTrans.getDelArrayList();
//				}
//				
//				DelArrayList.add(new String[]{TableName, datas[0], results[2] + HbaseInput.STRING_laccisplit + results[3]});
//				upDelTrans.setDelArrayList(DelArrayList);
//			} else {
//				datas[8] = results[7];
//				datas[9] = results[8];
//			}
//		}else{
//			upDelTrans.setUpValue(Array2String((datas[0]+","+Array2String(results)).split(",",-1)));
//			return upDelTrans;
//		}
//		upDelTrans.setUpValue(Array2String(datas));
//		return upDelTrans;
//	}

	public String Array2String(String[] datas) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < datas.length - 1; i++) {
			sb.append(datas[i]).append(",");
		}

		sb.append(datas[datas.length - 1]);
		return new String(sb);
	}
}
