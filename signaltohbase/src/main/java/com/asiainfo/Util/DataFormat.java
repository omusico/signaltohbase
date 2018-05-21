package com.asiainfo.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Get;

import com.asiainfo.Bean.IndexBean;
import com.asiainfo.Bean.SignalBean;
import com.asiainfo.HbaseDao.HbaseInput;

public class DataFormat {
	private ReentrantLock lock = new ReentrantLock();
	public ArrayList<Get> getList = new ArrayList<Get>();



	// [start]constant
	/**
	 * 手机号码位置
	 */
	public static final int LOC_MDN = 1;
	public static final int VOC_MO_MDN = 3;
	public static final int VOC_MT_MDN = 6;
	public static final int SMS_MO_MDN = 4;
	public static final int SMS_MT_MDN = 7;
	
	
	/**
	 * lac 和 ci 位置
	 */
	public static final int LOC_START_LAC = 8;
	public static final int LOC_START_CI = 9;
	public static final int LOC_END_LAC = 10;
	public static final int LOC_END_CI = 11;

	public static final int VOC_START_LAC = 12;
	public static final int VOC_START_CI = 13;
	public static final int VOC_END_LAC = 14;
	public static final int VOC_END_CI = 15;

	public static final int SMS_START_LAC = 12;
	public static final int SMS_START_CI = 13;
	
	/**
	 * imsi号码
	 */
	public static final int LOC_IMSI = 2;
	public static final int VOC_MO_IMSI = 1;
	public static final int VOC_MT_IMSI = 4;
	public static final int SMS_MO_IMSI = 2;
	public static final int SMS_MT_IMSI = 5;
	
	/**
	 * 主叫被叫
	 */
	public static final int VOC_CALLTYPE = 7;
	public static final int SMS_TYPE = 9;

	/**
	 * 时间，位置和短信只需要开始时间，使用结束时间判断当前记录时间是否大于入库记录的时间
	 */
	public static final int LOC_START_TIME = 0;
	public static final int LOC_END_TIME = 14;
	public static final int VOC_START_TIME = 0;
	public static final int VOC_END_TIME = 26;
	public static final int SMS_START_TIME = 0;
	public static final int SMS_END_TIME = 19;

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
	

	private static final String VOC_SIGNAL_TYPE = "1";
	private static final String VOC_RELOCATION = "5";
	private static final String VOC_MO_CALL = "9";
	private static final String VOC_MT_CALL = "10";
	private static final String VOC_E_CALL = "11";
	private static final String VOC_MOC = "0";
	private static final String VOC_MTC = "1";
	private static final String VOC_EMERGENT_CALL = "3";
	private static final String VOC_MOHO = "5";
	private static final String VOC_MTHO = "6";
	private static final int VOC_CALL_IMSI = 1;
	private static final int VOC_CALL_IMEI = 2;
	private static final int VOC_CALL = 3;
	private static final int VOC_CALLED_IMSI = 4;
	private static final int VOC_CALLED_IMEI = 5;
	private static final int VOC_CALLED = 6;

	
	
	// [end]

	public static final int signalType    = 0 ;
	public static final int area          = 1 ;
	public static final int roam          = 2 ;
	public static final int lac           = 3 ;
	public static final int ci            = 4 ;
	public static final int longitude     = 5 ;
	public static final int latitude      = 6 ;
	public static final int imsi          = 7 ;
	public static final int imei          = 8 ;
	public static final int currentCiTime = 9 ;
	public static final int lastTime      = 10;
	public static final int roamProv      = 11;
	public static final int roamCountry   = 12;
	
	
	
	// 0          1    2  3  4   5    6    7          8        9      10
	// 用户归属地 漫游地,LAC,CI,经度,纬度,IMSI,IMEI,进入当前CI时间,最后出现时间点,漫游省分,漫游国家,字段来源信令
	//Ps[0-11]  PsRoam[0-11]  CsMap[0-11]  ZbRoamout[0-11]  CsLoc[0-11]  CsVoc[0-11]  CsSms[0-11]  PsRoam[0&2-11]CsMap[1]
	public String PS_insert(String message) {
		
		String[] fields = message.split("\\^", -1);
		String UpdateTime = new TimeFormat().long2String(fields[8]);
		
//		StringBuffer sb = new StringBuffer();
//	    sb.append(fields[0]          ).append(",")
//		  .append("Ps[0-11]"     ).append(",")
//		  .append(fields[12]     ).append(",")
//		  .append(fields[13]     ).append(",")
//		  .append(fields[9]      ).append(",")
//		  .append(fields[10]     ).append(",")
//	      .append(fields[15]     ).append(",")
//		  .append(fields[16]     ).append(",")
//		  .append(fields[1]      ).append(",")
//		  .append(fields[2]      ).append(",")
//		  .append(UpdateTime     ).append(",")
//		  .append(UpdateTime     ).append(",")
//		  .append(""             ).append(",")
//		  .append(""  );
//		  
//	    String signalBean = new String(sb);
//
//		if ((fields[0] != null) && (!("").equals(fields[0]))) {
//			return signalBean;
//		}
		
		StringBuffer sb = new StringBuffer();
	    sb.append(fields[1]          ).append(",")
		  .append("Ps[0-11]"     ).append(",")
		  .append(fields[12]     ).append(",")
		  .append(fields[13]     ).append(",")
		  .append(fields[9]      ).append(",")
		  .append(fields[10]     ).append(",")
	      .append(fields[15]     ).append(",")
		  .append(fields[16]     ).append(",")
		  .append(fields[0]      ).append(",")
		  .append(fields[2]      ).append(",")
		  .append(UpdateTime     ).append(",")
		  .append(UpdateTime     ).append(",")
		  .append(""             ).append(",")
		  .append(""  );
		  
	    String signalBean = new String(sb);

		if ((fields[1] != null) && (!("").equals(fields[1]))) {
			return signalBean;
		}
		return null;
	}

	
	public String PS_roam_insert(String message){
		
		String[] fields = message.split("\\^", -1);
		String UpdateTime = new TimeFormat().long2String(fields[8]);
		
		StringBuffer sb = new StringBuffer();
	    sb.append(fields[0]          ).append(",")
		  .append("PsRoam[0-11]"   ).append(",")
		  .append(fields[12]         ).append(",")
		  .append(""         ).append(",")
		  .append(""          ).append(",")
		  .append(""           ).append(",")
	      .append(""    ).append(",")
		  .append(""     ).append(",")
		  .append(fields[1]         ).append(",")
		  .append(fields[2]         ).append(",")
		  .append("").append(",")
		  .append(UpdateTime     ).append(",")
		  .append(fields[24]     ).append(",")
		  .append(fields[25]  );
	    
	    String signalBean = new String(sb);

		if ((fields[0] != null) && (!("").equals(fields[0])) && ("CHN").equals(fields[25])) {
			return signalBean;
		}
		return null;
	}
	
	public String map_insert(String message){
		
		String[] fields = message.split("\\,", -1);
		String UpdateTime = fields[11];
		
		StringBuffer sb = new StringBuffer();
	    sb.append(fields[1]          ).append(",")
		  .append("CsMap[0-11]"   ).append(",")
		  .append(fields[14]         ).append(",")
		  .append(fields[17]         ).append(",")
		  .append(""          ).append(",")
		  .append(""           ).append(",")
	      .append(""    ).append(",")
		  .append(""     ).append(",")
		  .append(fields[3]         ).append(",")
		  .append(fields[4]         ).append(",")
		  .append("").append(",")
		  .append(UpdateTime     ).append(",")
		  .append(fields[16]     ).append(",")
		  .append(fields[15]  );
	    
	    String signalBean = new String(sb);
		
		if ((fields[1] != null) && (!("").equals(fields[1]))) {
			return signalBean;
		}
		return null;
	}
	
	public String zb_insert(String kafkaString){
		String[] fields = kafkaString.split("\\^", -1);
		  
		StringBuffer sb = new StringBuffer();
	    sb.append(fields[0]        ).append(",")
		  .append("ZbRoamout[0-11]").append(",")
		  .append(fields[12]       ).append(",")
		  .append(fields[11]       ).append(",")
		  .append(""               ).append(",")
		  .append(""               ).append(",")
	      .append(""               ).append(",")
		  .append(""               ).append(",")
		  .append(""               ).append(",")
		  .append(""               ).append(",")
		  .append(""               ).append(",")
		  .append(fields[10]       ).append(",")
		  .append(fields[3]        ).append(",")
		  .append("CHN"  );
	    
	    String signalString = new String(sb);
	    
		if ((fields[0] != null) && (!("").equals(fields[0]))){
			return signalString;
		}
		return null;
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
				return null;
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
//		String enterTime = fields[LOC_START_TIME].trim();
//		String mdn = fields[LOC_MDN].trim();
//		String lac = fields[LOC_START_LAC].trim();
//		String ci = fields[LOC_START_CI].trim();
//
//		// 如果start_ci不合法，则使用end_ci
//		if ("65535".equals(ci) || "".equals(ci)) {
//			ci = fields[LOC_END_CI].trim();
//			if ("65535".equals(ci) || "".equals(ci)) {
//				return null;
//			}
//
//			lac = fields[LOC_END_LAC].trim();
//			enterTime = fields[LOC_END_TIME].trim();
//		}
//
//		if (mdn.equals("")) {
//			return null;
//		}
//
//		String endTime = fields[LOC_END_TIME].trim();
//		
//		return ExtractData.extractData(mdn, "CsLoc[0-11]", ExtractData.LOC, lac, ci, enterTime, endTime, fields);
		String enterTime = fields[LOC_START_TIME].trim();
		String imsi = fields[ExtractData.LOC_IMSI].trim();
		String mdn = fields[LOC_MDN].trim();
		String lac = fields[LOC_START_LAC].trim();
		String ci = fields[LOC_START_CI].trim();

		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			ci = fields[LOC_END_CI].trim();
			if ("65535".equals(ci) || "".equals(ci)) {
				return null;
			}

			lac = fields[LOC_END_LAC].trim();
			enterTime = fields[LOC_END_TIME].trim();
		}

		if (imsi.equals("")) {
			return null;
		}

		String endTime = fields[LOC_END_TIME].trim();
		
		return ExtractData.extractData(mdn, "CsLoc[0-11]", ExtractData.LOC, lac, ci, enterTime, endTime, fields);
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
//		String enterTime = fields[VOC_START_TIME].trim();
//		String mdn = "";
//		String lac = fields[VOC_START_LAC].trim();
//		String ci = fields[VOC_START_CI].trim();
//		Integer callType = Integer.parseInt(fields[VOC_CALLTYPE].trim());
//
//		// 如果start_ci不合法，则使用end_ci
//		if ("65535".equals(ci) || "".equals(ci)) {
//			ci = fields[VOC_END_CI].trim();
//			if ("65535".equals(ci) || "".equals(ci)) {
//				return null;
//			}
//
//			lac = fields[VOC_END_LAC].trim();
//			enterTime = fields[VOC_END_TIME].trim();
//		}
//
//		int extraType = ExtractData.VOC_MO;
//
//		// 如果是主叫
//		if (callType == MO || callType == MOHO || callType == EMERGENT_CALL) {
//			mdn = fields[VOC_MO_MDN].trim();
//		} else if (callType == MT || callType == MTHO) {
//			// 如果是被叫
//			mdn = fields[VOC_MT_MDN].trim();
//			extraType = ExtractData.VOC_MT;
//		}
//
//		if (mdn.equals("")) {
//			return null;
//		}
//
//		String endTime = fields[VOC_END_TIME].trim();
//		
//		return ExtractData.extractData(mdn,"CsVoc[0-11]", extraType, lac, ci, enterTime, endTime, fields);
		
//		String enterTime = fields[VOC_START_TIME].trim();
//		String imsi= "";
//		String mdn = "";
//		String lac = fields[VOC_START_LAC].trim();
//		String ci = fields[VOC_START_CI].trim();
//		Integer callType = Integer.parseInt(fields[VOC_CALLTYPE].trim());
//
//		// 如果start_ci不合法，则使用end_ci
//		if ("65535".equals(ci) || "".equals(ci)) {
//			ci = fields[VOC_END_CI].trim();
//			if ("65535".equals(ci) || "".equals(ci)) {
//				return null;
//			}
//
//			lac = fields[VOC_END_LAC].trim();
//			enterTime = fields[VOC_END_TIME].trim();
//		}
//
//		int extraType = ExtractData.VOC_MO;
//
//		// 如果是主叫
//		if (callType == MO || callType == MOHO || callType == EMERGENT_CALL) {
//			mdn = fields[VOC_MO_MDN].trim();
//			imsi = fields[ExtractData.VOC_MO_IMSI].trim();
//		} else if (callType == MT || callType == MTHO) {
//			// 如果是被叫
//			mdn = fields[VOC_MT_MDN].trim();
//			imsi = fields[ExtractData.VOC_MT_IMSI].trim();
//			extraType = ExtractData.VOC_MT;
//		}
//
//		if (imsi.equals("")) {
//			return null;
//		}
//
//		String endTime = fields[VOC_END_TIME].trim();
//		
//		return ExtractData.extractData(mdn,"CsVoc[0-11]", extraType, lac, ci, enterTime, endTime, fields);
		
		String enterTime = fields[VOC_START_TIME].trim();
		String mdn = "";
		String imsi= "";
		String imei= "";
		String call_flag = "";
		String lac = fields[VOC_START_LAC].trim();
		String ci = fields[VOC_START_CI].trim();
		String business_type = fields[VOC_CALLTYPE].trim();
		String call_type = fields[SMS_TYPE].trim();
		
		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			ci = fields[VOC_END_CI].trim();
			if ("65535".equals(ci) || "".equals(ci)) {
				return null;
			}

			lac = fields[VOC_END_LAC].trim();
			enterTime = fields[VOC_END_TIME].trim();
		}

		int extraType = ExtractData.VOC_MO;
		
		if (business_type.equals(VOC_MO_CALL) || business_type.equals(VOC_E_CALL) || (business_type.equals(VOC_RELOCATION) && (call_type.equals(VOC_MOC) || call_type.equals(VOC_EMERGENT_CALL) || call_type.equals(VOC_MOHO)) ) ) {
			/* 主叫 */
			imsi = fields[VOC_CALL_IMSI];
			imei = fields[VOC_CALL_IMEI];
			mdn = fields[VOC_CALL];
			call_flag = "0";
		} else if (business_type.equals(VOC_MT_CALL) || (business_type.equals(VOC_RELOCATION) && (call_type.equals(VOC_MTC) || call_type.equals(VOC_MTHO)) ) ) {
			
			/* businssType=10,callType=0情况下主被叫修正 */
			if(VOC_MOC.equals(call_type)&&VOC_MT_CALL.equals(business_type)) {
				String call = fields[VOC_CALL];
				String callImsi = fields[VOC_CALL_IMSI];
				String callImei = fields[VOC_CALL_IMEI];
				String calledImsi = fields[VOC_CALLED_IMSI];
				String calledImei = fields[VOC_CALLED_IMEI];
				String called = fields[VOC_CALLED];
				
				/*判断被叫信息 called_imei或called_imsi是否正常 */
				boolean calledflag;
				if("".equals(calledImei)||calledImei.length()<14){
					calledflag=false;
				}else if("".equals(calledImsi)||
		                (calledImsi.startsWith("460")&&calledImsi.length()<15)||
		                ((!calledImsi.startsWith("460"))&&calledImsi.length()<12)){
					calledflag=false;
				}else{
					calledflag=true;
				}

				/*判断被叫信息 call、call_imsi、call_imei是否均正常)*/
				boolean callflag;
				if(callImei.length()<14){
					callflag=false;
				}else if(callImsi.startsWith("460")&&callImsi.length()==15&&call.length()>=11){
					callflag=true;
				}else if((!callImsi.startsWith("460"))&&callImsi.length()==12&&call.length()>8){
					callflag=true;
				}else{
					callflag=false;
				}
				
				if(calledflag){
					imsi = calledImsi;
					imei = calledImei;
					mdn = called;
					call_flag = "1";
				}else if(callflag){
					imsi = callImsi;
					imei = callImei;
					mdn = call;
					call_flag = "0";
				} else {
//					LOG.info(message);
//					LOG.info("This voice message is not MO or MT! The business_type is " + business_type + " and call_type is " + call_type + ". So, ignore it!");
					return null;
				}
			} else {
				/* 被叫 */
				imsi = fields[VOC_CALLED_IMSI];
				imei = fields[VOC_CALLED_IMEI];
				mdn = fields[VOC_CALLED];
				call_flag = "1";
			}
		}  else {
//			LOG.info(message);
//			LOG.info("This voice message is not MO or MT! The business_type is " + business_type + " and call_type is " + call_type + ". So, ignore it!");
			return null;
		}
		
		if("0".equals(call_flag)){
			extraType = ExtractData.VOC_MO;
		}else if("1".equals(call_flag)){
			extraType = ExtractData.VOC_MT;
		}
		
		if (imsi.equals("")) {
			return null;
		}

		String endTime = fields[VOC_END_TIME].trim();
	
		return ExtractData.extractData(mdn,"CsVoc[0-11]", extraType, lac, ci, enterTime, endTime, fields);

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
//		String enterTime = fields[SMS_START_TIME].trim();
//		String mdn = "";
//		String lac = fields[SMS_START_LAC].trim();
//		String ci = fields[SMS_START_CI].trim();
//		Integer smsType = Integer.parseInt(fields[SMS_TYPE].trim());
//
//		// 如果start_ci不合法，则使用end_ci
//		if ("65535".equals(ci) || "".equals(ci)) {
//			return null;
//		}
//
//		int extraType = ExtractData.SMS_MO;
//
//		// 如果是主叫
//		if (smsType == MO_SMS || smsType == STATUS_REPORT_SMS) {
//			mdn = fields[SMS_MO_MDN].trim();
//		} else if (smsType == MT_SMS) {
//			// 如果是被叫
//			mdn = fields[SMS_MT_MDN].trim();
//			extraType = ExtractData.SMS_MT;
//		} else {
//			return null;
//		}
//
//		if (mdn.equals("")) {
//			return null;
//		}
//
//		String endTime = fields[SMS_END_TIME].trim();
//
//
//		return ExtractData.extractData(mdn,"CsSms[0-11]",  extraType, lac, ci, enterTime, endTime, fields);
		String enterTime = fields[SMS_START_TIME].trim();
		String imsi= "";
		String mdn = "";
		String lac = fields[SMS_START_LAC].trim();
		String ci = fields[SMS_START_CI].trim();
		Integer smsType = Integer.parseInt(fields[SMS_TYPE].trim());

		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			return null;
		}

		int extraType = ExtractData.SMS_MO;

		// 如果是主叫
		if (smsType == MO_SMS || smsType == STATUS_REPORT_SMS) {
			mdn = fields[SMS_MO_MDN].trim();
			imsi = fields[ExtractData.SMS_MO_IMSI].trim();
		} else if (smsType == MT_SMS) {
			// 如果是被叫
			mdn = fields[SMS_MT_MDN].trim();
			imsi = fields[ExtractData.SMS_MT_IMSI].trim();
			extraType = ExtractData.SMS_MT;
		} else {
			return null;
		}

		if (imsi.equals("")) {
			return null;
		}

		String endTime = fields[SMS_END_TIME].trim();


		return ExtractData.extractData(mdn,"CsSms[0-11]",  extraType, lac, ci, enterTime, endTime, fields);
		
	}

	
	public SignalBean compareSignalBean(SignalBean signalBean) {
//		System.out.println("+++++++++++++++++++++++");
//		System.out.println(signalBean);
		
		List<String> kafkaSignalBean = signalBean.getKafkaValues();
		List<String> hbaseSignalBean = signalBean.getHbaseValues();
		
		if(hbaseSignalBean==null){
			if(!("").equals(kafkaSignalBean.get(4)) && !("").equals(kafkaSignalBean.get(5))){
				signalBean = AddInsIndexBean(signalBean);
			}
			return signalBean;
		}else{

			//match cs &
			Pattern pattern = Pattern.compile("(Cs(Loc|Sms|Voc)|(Ps))\\[0-11\\]");
			
			String kafkaSignalType = kafkaSignalBean.get(1);
			String hbaseSignalType = hbaseSignalBean.get(1);
			
			Boolean iskafkamatch = pattern.matcher(kafkaSignalType).find();
			Boolean ishbasematch = pattern.matcher(hbaseSignalType).find();
			
			
			
			if(new TimeFormat().Date2long(kafkaSignalBean.get(11)) 
					> new TimeFormat().Date2long(hbaseSignalBean.get(11))){
				//kafka中的信令最新
				
				if(!iskafkamatch && !ishbasematch){
					//hbase: PsRoam[0-11]  CsMap[0-11]  PsRoam[0&2-11]CsMap[1]  ZbRoamout[0-11]  
					//kafka: PsRoam[0-11]  CsMap[0-11]  PsRoam[0&2-11]CsMap[1]  ZbRoamout[0-11]  
					if(("PsRoam[0-11]").equals(kafkaSignalType) 
					&& !("PsRoam[0-11]").equals(hbaseSignalType)
					&& kafkaSignalBean.get(12).equals(hbaseSignalBean.get(12))
					){	
						//hbase: CsMap[0-11]  PsRoam[0&2-11]CsMap[1]  ZbRoamout[0-11]  
						//kafka: PsRoam[0-11]
						//hbase.roamProv == kafka.roamProv
						
						//PsRoam[0&2-11]CsMap[1]: 0 & 2-11 字段来自PsRoam, 1 字段来自 CsMap|Zbroamout
						kafkaSignalBean.set(3,hbaseSignalBean.get(3));
						kafkaSignalBean.set(1,"PsRoam[0&2-11]CsMap[1]");
					}
				}else {
					//hbase: Ps[0-11]  CsLoc[0-11]  CsVoc[0-11]  CsSms[0-11]
					//kafka: Ps[0-11]  CsLoc[0-11]  CsVoc[0-11]  CsSms[0-11]
					if((kafkaSignalBean.get(4).equals(hbaseSignalBean.get(4)) 
					&& kafkaSignalBean.get(5).equals(hbaseSignalBean.get(5)))){
						kafkaSignalBean.set(9,hbaseSignalBean.get(9));
						kafkaSignalBean.set(10,hbaseSignalBean.get(10));
					}else{
						if(signalBean.getIsGet()){
							if(ishbasematch){
								//delIndexBean for Delete hbaseIndex
								signalBean = AddDelIndexBean(signalBean);

							}
							if(iskafkamatch){
								//insIndexBean for Insert hbaseIndex
								signalBean = AddInsIndexBean(signalBean);
							}
						}

					}
				}
				
				signalBean.setKafkaValues(kafkaSignalBean);
				return signalBean;
				
			}else{
				//hbase中的信令最新
				
				if(!iskafkamatch && !ishbasematch){
					//hbase: PsRoam[0-11]  CsMap[0-11]  ZbRoamout[0-11]  PsRoam[0&2-11]CsMap[1]
					//kafka: PsRoam[0-11]  CsMap[0-11]  ZbRoamout[0-11]  PsRoam[0&2-11]CsMap[1]
					if(("PsRoam[0-11]").equals(hbaseSignalType) 
					&& !("PsRoam[0-11]").equals(kafkaSignalType)
					&& kafkaSignalBean.get(12).equals(hbaseSignalBean.get(12))
					){  //hbase中的信令最新 hbase中的是PsRoam & kafka中的是CsMap & 两者的漫游省分相同：此为特殊情况，用kafka中的CsMap的漫游地更新hbase中PsRoam的漫游地
						//hbase: PsRoam[0-11]  
						//kafka: CsMap[0-11]  PsRoam[0&2-11]CsMap[1]  ZbRoamout[0-11]
						//hbase.roamProv == kafka.roamProv
						
						hbaseSignalBean.set(3,kafkaSignalBean.get(3));
						hbaseSignalBean.set(1,"PsRoam[0&2-11]CsMap[1]");
					}
				}else{
					//hbase: Ps[0-11]  CsLoc[0-11]  CsVoc[0-11]  CsSms[0-11]
					//kafka: Ps[0-11]  CsLoc[0-11]  CsVoc[0-11]  CsSms[0-11]
					if((kafkaSignalBean.get(4).equals(hbaseSignalBean.get(4)) 
					&& kafkaSignalBean.get(5).equals(hbaseSignalBean.get(5)))
					&& (new TimeFormat().Date2long(kafkaSignalBean.get(10)) 
						< new TimeFormat().Date2long(hbaseSignalBean.get(10)))
					){ //kafka.lac == hbase.lac & kafka.ci == hbase.ci & kafka.currentCiTime < hbase.currentCiTime
						hbaseSignalBean.set(10,kafkaSignalBean.get(10));
					}
				}
				signalBean.setKafkaValues(hbaseSignalBean);
				return signalBean;
				
			}
		}
	}
	
	private SignalBean AddDelIndexBean(SignalBean signalBean){
		//delIndexBean for Delete hbaseIndex
		List<IndexBean> delIndexBeanList = signalBean.getDelIndexBeanList();
		List<String> hbaseSignalBean = signalBean.getHbaseValues();
		IndexBean delIndexBean = new IndexBean();
		delIndexBean.setTable(ParamUtil.TABLE_NAME_INDEX);
		delIndexBean.setRowkey(hbaseSignalBean.get(4) + HbaseInput.STRING_laccisplit + hbaseSignalBean.get(5));
		delIndexBean.setCf(HbaseInput.STRING_f);
		delIndexBean.setCq(hbaseSignalBean.get(0));
		
		delIndexBeanList.add(delIndexBean);
		signalBean.setDelIndexBeanList(delIndexBeanList);
		
		return signalBean;
	}
	
	private SignalBean AddInsIndexBean(SignalBean signalBean){
		//insIndexBean for Insert hbaseIndex
		List<IndexBean> insIndexBeanList = signalBean.getInsIndexBeanList();
		List<String> kafkaSignalBean = signalBean.getKafkaValues();
		
		
		IndexBean insIndexBean = new IndexBean();
		insIndexBean.setTable(ParamUtil.TABLE_NAME_INDEX);
		insIndexBean.setRowkey(kafkaSignalBean.get(4) + HbaseInput.STRING_laccisplit + kafkaSignalBean.get(5));
		insIndexBean.setCf(HbaseInput.STRING_f);
		insIndexBean.setCq(kafkaSignalBean.get(0));
		insIndexBean.setTs(new TimeFormat().Date2long(kafkaSignalBean.get(11)));
		insIndexBean.setValue(kafkaSignalBean.get(2));				
		
		insIndexBeanList.add(insIndexBean);
		signalBean.setInsIndexBeanList(insIndexBeanList);
		
		return signalBean;
	}

	public String Array2String(String[] datas) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < datas.length - 1; i++) {
			sb.append(datas[i]).append(",");
		}

		sb.append(datas[datas.length - 1]);
		return new String(sb);
	}

}
