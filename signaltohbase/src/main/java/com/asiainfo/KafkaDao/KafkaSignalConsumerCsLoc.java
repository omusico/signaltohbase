package com.asiainfo.KafkaDao;

public class KafkaSignalConsumerCsLoc extends KafkaSignalConsumer {

	
	public KafkaSignalConsumerCsLoc(String topic, int partition_num) {
		super(topic, partition_num);
	}
	
	@Override
	public String signalFormat(String kafkaString){
		String[] kafkaStringFields = kafkaString.split(LOC_SPLIT, -1);
		
		String mdn          = getStringByIndex(kafkaStringFields, LOC_MDN).trim();
		String signaltype   = LOC_SIGNALTYPE;
		String homeArea     = getStringByIndex(kafkaStringFields, LOC_HOST_AREA).trim();
		String roamArea     = getStringByIndex(kafkaStringFields, LOC_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, LOC_START_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, LOC_START_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, LOC_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, LOC_LAT).trim();
		String imsi         = getStringByIndex(kafkaStringFields, LOC_IMSI).trim();
		String imei         = getStringByIndex(kafkaStringFields, LOC_IMEI).trim();
		String firstciTime  = getStringByIndex(kafkaStringFields, LOC_START_TIME).trim();
		String lastTime     = getStringByIndex(kafkaStringFields, LOC_LAST_TIME).trim();
		String roamCity     = RoamAreaFormat(getStringByIndex(kafkaStringFields, LOC_ROAM_CITY).trim());
		String roamProv     = getStringByIndex(kafkaStringFields, LOC_ROAM_PROV).trim();
		String roamCountry  = getStringByIndex(kafkaStringFields, LOC_ROAM_COUNTRY).trim();
		
		// 如果start_ci不合法，则使用end_ci
		if ("65535".equals(ci) || "".equals(ci)) {
			ci = getStringByIndex(kafkaStringFields, LOC_END_CI);
			if ("65535".equals(ci) || "".equals(ci)) {
				return "";
			}

			lac = getStringByIndex(kafkaStringFields, LOC_END_LAC);
		}
		
		return combinString(mdn, signaltype, homeArea, roamArea, lac, ci, lon, lat, imsi, imei, firstciTime, lastTime, roamCity, roamProv, roamCountry);
	}

}
