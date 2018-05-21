package com.asiainfo.KafkaDao;

public class KafkaSignaConsumerZbRoamput extends KafkaSignalConsumer {

	public KafkaSignaConsumerZbRoamput(String topic, int partition_num) {
		super(topic, partition_num);
	}
	
	@Override
	public String signalFormat(String kafkaString){
	    
		String[] kafkaStringFields = kafkaString.split(ZBROAM_SPLIT, -1);
		
		String mdn          = getStringByIndex(kafkaStringFields, ZBROAM_MDN).trim();;
		String signaltype   = ZBROAM_SIGNALTYPE;
		String area         = getStringByIndex(kafkaStringFields, ZBROAM_HOST_AREA).trim();
		String roaming      = getStringByIndex(kafkaStringFields, ZBROAM_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, ZBROAM_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, ZBROAM_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, ZBROAM_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, ZBROAM_LAT).trim();
		String imsi         = getStringByIndex(kafkaStringFields, ZBROAM_IMSI).trim();
		String imei         = getStringByIndex(kafkaStringFields, ZBROAM_IMEI).trim();
		String currentTime  = getStringByIndex(kafkaStringFields, ZBROAM_START_TIME).trim();
		String lastTime     = getStringByIndex(kafkaStringFields, ZBROAM_END_TIME);
		String roamCity     = getStringByIndex(kafkaStringFields, ZBROAM_ROAM_CITY).trim();
		String roamProv     = getStringByIndex(kafkaStringFields, ZBROAM_ROAM_PROV).trim();
		String roamCountry  = ZBROAM_ROAM_COUNTRY;
		
		return combinString(mdn, signaltype, area, roaming, lac, ci, lon, lat, imsi, imei, currentTime, lastTime, roamCity, roamProv, roamCountry);

	}

}
