package com.asiainfo.KafkaDao;

public class KafkaSignalConsumerCsMap extends KafkaSignalConsumer {

	public KafkaSignalConsumerCsMap(String topic, int partition_num) {
		super(topic, partition_num);
	}
	
	@Override
	public String signalFormat(String kafkaString){		
		String[] kafkaStringFields = kafkaString.split(MAP_SPLIT, -1);
		
		String mdn          = getStringByIndex(kafkaStringFields, MAP_MDN).trim();;
		String signaltype   = MAP_SIGNALTYPE;
		String area         = getStringByIndex(kafkaStringFields, MAP_HOST_AREA).trim();
		String roaming      = getStringByIndex(kafkaStringFields, MAP_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, MAP_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, MAP_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, MAP_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, MAP_LAT).trim();
		String imsi         = getStringByIndex(kafkaStringFields, MAP_IMSI).trim();
		String imei         = getStringByIndex(kafkaStringFields, MAP_IMEI).trim();
		String currentTime  = getStringByIndex(kafkaStringFields, MAP_START_TIME).trim();
		String lastTime     = getStringByIndex(kafkaStringFields, MAP_END_TIME).trim();
		String roamCity     = getStringByIndex(kafkaStringFields, MAP_ROAM_CITY).trim();
		String roamProv     = getStringByIndex(kafkaStringFields, MAP_ROAM_PROV).trim();
		String roamCountry  = getStringByIndex(kafkaStringFields, MAP_ROAM_COUNTRY).trim();
		
		return combinString(mdn, signaltype, area, roaming, lac, ci, lon, lat, imsi, imei, currentTime, lastTime, roamCity, roamProv, roamCountry);
	}
}
