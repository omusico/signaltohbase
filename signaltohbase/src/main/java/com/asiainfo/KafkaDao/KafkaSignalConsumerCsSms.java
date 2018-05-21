package com.asiainfo.KafkaDao;

public class KafkaSignalConsumerCsSms extends KafkaSignalConsumer {

	public KafkaSignalConsumerCsSms(String topic, int partition_num) {
		super(topic, partition_num);
	}

	@Override
	public String signalFormat(String kafkaString){
		String[] kafkaStringFields = kafkaString.split(SMS_SPLIT, -1);
		
		String mdn          = "";
		String signaltype   = SMS_SIGNALTYPE;
		String homeArea     = getStringByIndex(kafkaStringFields, SMS_HOST_AREA).trim();
		String roamArea     = getStringByIndex(kafkaStringFields, SMS_ROAM_AREA).trim();
		String lac          = getStringByIndex(kafkaStringFields, SMS_START_LAC).trim();
		String ci           = getStringByIndex(kafkaStringFields, SMS_START_CI).trim();
		String lon          = getStringByIndex(kafkaStringFields, SMS_LON).trim();
		String lat          = getStringByIndex(kafkaStringFields, SMS_LAT).trim();
		String imsi         = "";
		String imei         = "";
		String firstciTime  = getStringByIndex(kafkaStringFields, SMS_START_TIME).trim();
		String lastTime     = getStringByIndex(kafkaStringFields, SMS_END_TIME).trim();
		String roamCity     = RoamAreaFormat(getStringByIndex(kafkaStringFields, SMS_ROAM_CITY).trim());
		String roamProv     = getStringByIndex(kafkaStringFields, SMS_ROAM_PROV).trim();
		String roamCountry  = getStringByIndex(kafkaStringFields, SMS_ROAM_COUNTRY).trim();

		Integer smsType = Integer.parseInt(getStringByIndex(kafkaStringFields, SMS_TYPE).trim());

		// 如果start_ci不合法，则剔除
		if ("65535".equals(ci) || "".equals(ci)) {
			return "";
		}

		// 如果是主叫
		if (smsType == MO_SMS || smsType == STATUS_REPORT_SMS) {
			mdn  = getStringByIndex(kafkaStringFields, SMS_MO_MDN).trim();
			imsi = getStringByIndex(kafkaStringFields, SMS_MO_IMSI).trim();
			imei = getStringByIndex(kafkaStringFields, SMS_MO_IMEI).trim();
		} else if (smsType == MT_SMS) {
			// 如果是被叫
			mdn = getStringByIndex(kafkaStringFields, SMS_MT_MDN).trim();
			imsi = getStringByIndex(kafkaStringFields, SMS_MT_IMSI).trim();
			imei = getStringByIndex(kafkaStringFields, SMS_MT_IMEI).trim();
		} else {
			return "";
		}

		return combinString(mdn, signaltype, homeArea, roamArea, lac, ci, lon, lat, imsi, imei, firstciTime, lastTime, roamCity, roamProv, roamCountry);
	}
}
