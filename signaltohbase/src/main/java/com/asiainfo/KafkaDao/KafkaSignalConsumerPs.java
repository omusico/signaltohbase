package com.asiainfo.KafkaDao;

import com.asiainfo.Util.TimeFormat;

public class KafkaSignalConsumerPs extends KafkaSignalConsumer {

	public KafkaSignalConsumerPs(String topic, int partition_num) {
		super(topic, partition_num);
	}
	
	@Override
	public String signalFormat(String kafkaString){
		String[] kafkaStringFields = kafkaString.split(PS_SPLIT, -1);

		String trans_req_time_sec = new TimeFormat().long2String(
				getStringByIndex(kafkaStringFields, PS_END_TIME).trim());
		
		String mdn          = getStringByIndex(kafkaStringFields, PS_MDN).trim();;
		String signaltype   = PS_SIGNALTYPE;
		String homeArea     = getStringByIndex(kafkaStringFields, PS_HOST_AREA).trim();
		String roamArea     = getStringByIndex(kafkaStringFields, PS_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, PS_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, PS_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, PS_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, PS_LAT).trim();
		String imsi         = getStringByIndex(kafkaStringFields, PS_IMSI).trim();
		String imei         = getStringByIndex(kafkaStringFields, PS_IMEI).trim();
		String firstciTime  = trans_req_time_sec;
		String lastTime     = trans_req_time_sec;
		String roamCity     = getStringByIndex(kafkaStringFields, PS_ROAM_CITY).trim();
		String roamProv     = getStringByIndex(kafkaStringFields, PS_ROAM_PROV).trim();
		String roamCountry  = getStringByIndex(kafkaStringFields, PS_ROAM_COUNTRY).trim();
		
		return combinString(mdn, signaltype, homeArea, roamArea, lac, ci, lon, lat, imsi, imei, firstciTime, lastTime, roamCity, roamProv, roamCountry);
	}
}
