package com.asiainfo.KafkaDao;

import com.asiainfo.Util.TimeFormat;

public class KafkaSignalConsumerPsRoam extends KafkaSignalConsumer {

	public KafkaSignalConsumerPsRoam(String topic, int partition_num) {
		super(topic, partition_num);
	}

	@Override
	public String signalFormat(String kafkaString){
		String[] kafkaStringFields = kafkaString.split(PSROAM_SPLIT, -1);

		String trans_req_time_sec = new TimeFormat().long2String(
				getStringByIndex(kafkaStringFields, PSROAM_END_TIME).trim());
		
		String mdn          = getStringByIndex(kafkaStringFields, PSROAM_MDN).trim();;
		String signaltype   = PSROAM_SIGNALTYPE;
		String homeArea     = getStringByIndex(kafkaStringFields, PSROAM_HOST_AREA).trim();
		String roamArea     = getStringByIndex(kafkaStringFields, PSROAM_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, PSROAM_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, PSROAM_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, PSROAM_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, PSROAM_LAT).trim();
		String imsi         = getStringByIndex(kafkaStringFields, PSROAM_IMSI).trim();
		String imei         = getStringByIndex(kafkaStringFields, PSROAM_IMEI).trim();
		String firstciTime  = getStringByIndex(kafkaStringFields, PSROAM_START_TIME).trim();
		String lastTime     = trans_req_time_sec;
		String roamCity     = getStringByIndex(kafkaStringFields, PSROAM_ROAM_CITY).trim();
		String roamProv     = getStringByIndex(kafkaStringFields, PSROAM_ROAM_PROV).trim();
		String roamCountry  = getStringByIndex(kafkaStringFields, PSROAM_ROAM_COUNTRY).trim();
		
		return combinString(mdn, signaltype, homeArea, roamArea, lac, ci, lon, lat, imsi, imei, firstciTime, lastTime, roamCity, roamProv, roamCountry);
	}
}
