package com.asiainfo.KafkaDao;

public class KafkaSignalConsumerCsVoc extends KafkaSignalConsumer {

	public KafkaSignalConsumerCsVoc(String topic, int partition_num) {
		super(topic, partition_num);
	}

	@Override
	public String signalFormat(String kafkaString){
		String[] kafkaStringFields = kafkaString.split(VOC_SPLIT, -1);
		
		String mdn          = "";
		String signaltype   = VOC_SIGNALTYPE;
		String homeArea     = getStringByIndex(kafkaStringFields, VOC_HOST_AREA).trim();
		String roamArea     = getStringByIndex(kafkaStringFields, VOC_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, VOC_START_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, VOC_START_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, VOC_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, VOC_LAT).trim();
		String imsi         = "";
		String imei         = "";
		String firstciTime  = getStringByIndex(kafkaStringFields, VOC_START_TIME).trim();
		String lastTime     = getStringByIndex(kafkaStringFields, VOC_END_TIME).trim();
		String roamCity     = RoamAreaFormat(getStringByIndex(kafkaStringFields, VOC_ROAM_CITY).trim());
		String roamProv     = getStringByIndex(kafkaStringFields, VOC_ROAM_PROV).trim();
		String roamCountry  = getStringByIndex(kafkaStringFields, VOC_ROAM_COUNTRY).trim();

		
		String call_flag = "";
		String business_type = getStringByIndex(kafkaStringFields, VOC_BUSINESS_TYPE).trim();
		String call_type     = getStringByIndex(kafkaStringFields, VOC_CALL_TYPE).trim();
		
		String call =        getStringByIndex(kafkaStringFields, VOC_MO_MDN);
		String callImsi =    getStringByIndex(kafkaStringFields, VOC_MO_IMSI);
		String callImei =    getStringByIndex(kafkaStringFields, VOC_MO_IMEI);
		String called =      getStringByIndex(kafkaStringFields, VOC_MT_MDN);
		String calledImsi =  getStringByIndex(kafkaStringFields, VOC_MT_IMSI);
		String calledImei =  getStringByIndex(kafkaStringFields, VOC_MT_IMEI);
		
		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			ci = getStringByIndex(kafkaStringFields, VOC_END_CI).trim();
			if ("65535".equals(ci) || "".equals(ci)) {
				return null;
			}

			lac = getStringByIndex(kafkaStringFields, VOC_END_LAC).trim();
			firstciTime = getStringByIndex(kafkaStringFields, VOC_END_TIME).trim();
		}		
		
		if (business_type.equals(VOC_MO_CALL) || business_type.equals(VOC_E_CALL) || (business_type.equals(VOC_RELOCATION) && (call_type.equals(VOC_MOC) || call_type.equals(VOC_EMERGENT_CALL) || call_type.equals(VOC_MOHO)) ) ) {
			/* 主叫 */
			call_flag = "0";
		} else if (business_type.equals(VOC_MT_CALL) || (business_type.equals(VOC_RELOCATION) && (call_type.equals(VOC_MTC) || call_type.equals(VOC_MTHO)) ) ) {
			
			/* businssType=10,callType=0情况下主被叫修正 */
			if(VOC_MOC.equals(call_type)&&VOC_MT_CALL.equals(business_type)) {

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
					return "";
				}
			} else {
				/* 被叫 */
				call_flag = "1";
			}
		}  else {
			return "";
		}
		
		if("0".equals(call_flag)){
			/* 主叫 */
			mdn = getStringByIndex(kafkaStringFields, VOC_MO_MDN);
			imsi = getStringByIndex(kafkaStringFields, VOC_MO_IMSI);
			imei = getStringByIndex(kafkaStringFields, VOC_MO_IMEI);
		}else if("1".equals(call_flag)){
			/* 被叫 */
			mdn = getStringByIndex(kafkaStringFields, VOC_MT_MDN);
			imsi = getStringByIndex(kafkaStringFields, VOC_MT_IMSI);
			imei = getStringByIndex(kafkaStringFields, VOC_MT_IMEI);
		}
		
		if (imsi.equals("")) {
			return null;
		}

		return combinString(mdn, signaltype, homeArea, roamArea, lac, ci, lon, lat, imsi, imei, firstciTime, lastTime, roamCity, roamProv, roamCountry);
	}
}
