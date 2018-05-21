package com.asiainfo.KafkaDao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.ParamUtil;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaSignalConsumer extends Thread{
	
	// [start]constant
	/**
	 * 信令分隔符
	 */
	public static final String LOC_SPLIT = ",";
	public static final String VOC_SPLIT = ",";
	public static final String SMS_SPLIT = ",";
	public static final String PS_SPLIT = "\\^";
	public static final String PSROAM_SPLIT = "\\^";
	public static final String MAP_SPLIT = "\\,";
	public static final String ZBROAM_SPLIT = "\\^";
	
	/**
	 * 手机号码索引
	 */
	//msisdn
	public static final int LOC_MDN = 1;
	public static final int VOC_MO_MDN = 3;
	public static final int VOC_MT_MDN = 6;
	public static final int SMS_MO_MDN = 4;
	public static final int SMS_MT_MDN = 7;
	public static final int PS_MDN = 0;
	public static final int PSROAM_MDN = 0;
	public static final int MAP_MDN = 2;
	public static final int ZBROAM_MDN = 0;

	
	/**
	 * 信令类型索引
	 */
	public static final String LOC_SIGNALTYPE = "CsLoc[0-11]";
	public static final String VOC_SIGNALTYPE = "CsVoc[0-11]";
	public static final String SMS_SIGNALTYPE = "CsSms[0-11]";
	public static final String PS_SIGNALTYPE = "Ps[0-11]";
	public static final String PSROAM_SIGNALTYPE = "PsRoam[0-11]";
	public static final String MAP_SIGNALTYPE = "CsMap[0-11]";
	public static final String ZBROAM_SIGNALTYPE = "ZbRoamout[0-11]";
	
	
	/**
	 * 用户归属地索引
	 */
	//areacode
	public static final int LOC_HOST_AREA = 47;
	public static final int VOC_HOST_AREA = 46;
	public static final int SMS_HOST_AREA = 32;
	public static final int PS_HOST_AREA = 12;
	public static final int PSROAM_HOST_AREA = 12;
	public static final int MAP_HOST_AREA = 14;
	public static final int ZBROAM_HOST_AREA = 12;

	/**
	 * 用户漫游区县索引
	 */
	//end_area_id
	public static final int LOC_ROAM_AREA = 33;
	public static final int VOC_ROAM_AREA = 40;
	public static final int SMS_ROAM_AREA = 24;
	public static final int PS_ROAM_AREA = 14;
	public static final int PSROAM_ROAM_AREA = -1;
	public static final int MAP_ROAM_AREA = -1;
	public static final int ZBROAM_ROAM_AREA = -1;
	
	/**
	 * lac 索引
	 */
	public static final int LOC_START_LAC = 8;
	public static final int LOC_END_LAC = 10;
	public static final int VOC_START_LAC = 12;
	public static final int VOC_END_LAC = 14;
	public static final int SMS_START_LAC = 12;
	public static final int PS_LAC = 9;
	public static final int PSROAM_LAC = -1;
	public static final int MAP_LAC = -1;
	public static final int ZBROAM_LAC = -1;

	
	/**
	 * ci 索引
	 */
	public static final int LOC_START_CI = 9;
	public static final int LOC_END_CI = 11;
	public static final int VOC_START_CI = 13;
	public static final int VOC_END_CI = 15;
	public static final int SMS_START_CI = 13;
	public static final int PS_CI = 10;
	public static final int PSROAM_CI = -1;
	public static final int MAP_CI = -1;
	public static final int ZBROAM_CI = -1;
	
	/**
	 * 经度 索引
	 */
	//end_lon 
	public static final int LOC_LON = 35;  
	public static final int VOC_LON = 42;
	public static final int SMS_LON = 26;
	public static final int PS_LON = 15;
	public static final int PSROAM_LON = -1;
	public static final int MAP_LON = -1;
	public static final int ZBROAM_LON = -1;
	
	/**
	 * 纬度 索引
	 */
	//end_lat
	public static final int LOC_LAT = 36;
	public static final int VOC_LAT = 43;
	public static final int SMS_LAT = 27;
	public static final int PS_LAT = 16;
	public static final int PSROAM_LAT = -1;
	public static final int MAP_LAT = -1;
	public static final int ZBROAM_LAT = -1;
	
	/**
	 * imsi号码索引
	 */
	//imsi
	public static final int LOC_IMSI = 2;
	public static final int VOC_MO_IMSI = 1;
	public static final int VOC_MT_IMSI = 4;
	public static final int SMS_MO_IMSI = 2;
	public static final int SMS_MT_IMSI = 5;
	public static final int PS_IMSI = 1;
	public static final int PSROAM_IMSI = 1;
	public static final int MAP_IMSI = 3;
	public static final int ZBROAM_IMSI = -1;
	
	/**
	 * imei号码索引
	 */
	//imei
	public static final int LOC_IMEI = 3;
	public static final int VOC_MO_IMEI = 2;
	public static final int VOC_MT_IMEI = 5;
	public static final int SMS_MO_IMEI = 3;
	public static final int SMS_MT_IMEI = 6;
	public static final int PS_IMEI = 2;
	public static final int PSROAM_IMEI = 2;
	public static final int MAP_IMEI = 4;
	public static final int ZBROAM_IMEI = 4;
	
	//时间，位置和短信只需要开始时间，使用结束时间判断当前记录时间是否大于入库记录的时间
	/**
	 * 进入当前ci时间索引
	 */
	/**
	 * 最后出现时间索引
	 */
	/**
	 * 时间，位置和短信只需要开始时间，使用结束时间判断当前记录时间是否大于入库记录的时间
	 */
	//start_time
	public static final int LOC_START_TIME = 0;
	//end_time
	public static final int LOC_END_TIME = 14;
	
	public static final int VOC_START_TIME = 0;
	public static final int VOC_END_TIME = 26;
	public static final int SMS_START_TIME = 0;
	public static final int SMS_END_TIME = 19;
	public static final int PS_END_TIME = 8;
	public static final int PSROAM_START_TIME = -1;
	public static final int PSROAM_END_TIME = 8;
	public static final int MAP_START_TIME = -1;
	public static final int MAP_END_TIME = 11;
	public static final int ZBROAM_START_TIME = -1;
	public static final int ZBROAM_END_TIME = 10;
	
	

	//end_time
	public static final int LOC_LAST_TIME = 14;
		
	/**
	 * 漫游省分索引
	 */
	//end_province_id
	public static final int LOC_ROAM_PROV = -1;
	//province_id
	public static final int VOC_ROAM_PROV = -1;
	//come_province
	public static final int SMS_ROAM_PROV = -1;

	public static final int PS_ROAM_PROV = -1;
	
	public static final int PSROAM_ROAM_PROV = 24;
	
	public static final int MAP_ROAM_PROV = 16;
	
	public static final int ZBROAM_ROAM_PROV = 3;
	
	/**
	 * 漫游国际索引
	 */
	public static final int LOC_ROAM_COUNTRY = -1;
	//country_id
	public static final int VOC_ROAM_COUNTRY = -1;
	public static final int SMS_ROAM_COUNTRY = -1;

	public static final int PS_ROAM_COUNTRY = -1;
	
	public static final int PSROAM_ROAM_COUNTRY = 25;
	
	public static final int MAP_ROAM_COUNTRY = 15;
	
	public static final String ZBROAM_ROAM_COUNTRY = "CHN";
	
	/**
	 * 漫游地市索引
	 */
	public static final int LOC_ROAM_CITY = -1;
	public static final int VOC_ROAM_CITY = -1;
	public static final int SMS_ROAM_CITY = -1;
	public static final int PS_ROAM_CITY = -1;
	public static final int PSROAM_ROAM_CITY = 13;
	public static final int MAP_ROAM_CITY = 17;
	public static final int ZBROAM_ROAM_CITY = 11;
	
	/**
	 * 主叫被叫
	 */
	public static final int VOC_BUSINESS_TYPE = 7;
	public static final int VOC_CALL_TYPE = 8;
	//短信业务类型
	public static final int SMS_TYPE = 9;


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
	
	public static final String VOC_RELOCATION = "5";
	public static final String VOC_MO_CALL = "9";
	public static final String VOC_MT_CALL = "10";
	public static final String VOC_E_CALL = "11";
	public static final String VOC_MOC = "0";
	public static final String VOC_MTC = "1";
	public static final String VOC_EMERGENT_CALL = "3";
	public static final String VOC_MOHO = "5";
	public static final String VOC_MTHO = "6";

//	public static final int signalType    = 0 ;
//	public static final int area          = 1 ;
//	public static final int roam          = 2 ;
//	public static final int lac           = 3 ;
//	public static final int ci            = 4 ;
//	public static final int longitude     = 5 ;
//	public static final int latitude      = 6 ;
//	public static final int imsi          = 7 ;
//	public static final int imei          = 8 ;
//	public static final int currentCiTime = 9 ;
//	public static final int lastTime      = 10;
//	public static final int roamProv      = 11;
//	public static final int roamCountry   = 12;
	
	// [end]

	ParamUtil paramUtil = ParamUtil.getInstance();
	Logger logger = Log4JUtil.getLogger();
	private ReentrantLock lock = new ReentrantLock();

	private String topic;
	private int partition_num;
	private static KafkaOutput kafkaOutput;

	public KafkaSignalConsumer(String topic ,int partition_num){
		super();
		this.topic = topic;
		this.partition_num = partition_num;
	}
	
	public static void InitKafkaOutput(KafkaOutput kafkaOutputImpl){
		kafkaOutput = kafkaOutputImpl;
	}
	
	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(partition_num)); 
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topic);
		ExecutorService executor = Executors.newFixedThreadPool(partition_num);  
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new KafkaConsumerThread(stream));  
        }  
	}
	
	class KafkaConsumerThread extends Thread {
		private KafkaStream<byte[], byte[]> stream; 
		HashMap<String, Integer> num = new HashMap<String,Integer>();
	 	public KafkaConsumerThread(KafkaStream<byte[], byte[]> stream){
			this.stream = stream; 
		}
		
		@Override
		public void run() {
	        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();  
	        while (iterator.hasNext()) { 
				MessageAndMetadata<byte[], byte[]> mam = iterator.next();
				
				//将kafka取出的字符串格式化处理
				String signalFormat = null;
				lock.lock();
				try{
					signalFormat =  signalFormat(new String(mam.message()));
				} finally {
					lock.unlock();
				}
				if(!(signalFormat==null || "".equals(signalFormat))){
					logger.debug("formatdSignal: " + signalFormat);
					kafkaOutput.fillCollToInput(signalFormat);
				}
	        }
		}
	}
	
	public String signalFormat(String kafkaString){
		return null;
	}
	
	public String getStringByIndex(String[] stringFields, int index){
		if(index>=0){
			return stringFields[index].trim();
		}else{
			return "";
		}
	}
	
	public static String combinString(String mdn,
			                          String signaltype,
			                          String homeArea,
			                          String roamArea,
			                          String lac,
			                          String ci,
			                          String lon,
			                          String lat,
			                          String imsi,
			                          String imei,
			                          String firstciTime,
			                          String lastTime,
			                          String roamCity,
			                          String roamProv,
			                          String roamCountry) {
		StringBuffer sb = new StringBuffer();
		sb.append("");
		
		if(imsi.length()>3 && !imsi.substring(0, 3).equals("460") && !"000".equals(homeArea)){
			//入境用户
			
		    sb.append(imsi       ).append(",")
			  .append(signaltype ).append(",")
			  .append(homeArea   ).append(",")
			  .append(roamArea   ).append(",")
			  .append(lac        ).append(",")
			  .append(ci         ).append(",")
			  .append(lon        ).append(",")
			  .append(lat        ).append(",")
		      .append(imsi       ).append(",")
			  .append(imei       ).append(",")
			  .append(firstciTime).append(",")
			  .append(lastTime   ).append(",")
			  .append(roamProv   ).append(",")
			  .append(roamCountry).append(",")
			  .append(roamCity   ).append(",")
			  .append(mdn        );
		}else {
			//国内用户 或 其他用户
			if("".equals(mdn))
				return "";
			
		    sb.append(mdn        ).append(",")
			  .append(signaltype ).append(",")
			  .append(homeArea   ).append(",")
			  .append(roamArea   ).append(",")
			  .append(lac        ).append(",")
			  .append(ci         ).append(",")
			  .append(lon        ).append(",")
			  .append(lat        ).append(",")
		      .append(imsi       ).append(",")
			  .append(imei       ).append(",")
			  .append(firstciTime).append(",")
			  .append(lastTime   ).append(",")
			  .append(roamProv   ).append(",")
			  .append(roamCountry).append(",")
			  .append(roamCity   ).append(",")
			  .append(mdn        );
		}

		return new String(sb);
		
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
	
	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();

		properties.put("zookeeper.connect", ParamUtil.KAFKA_ZOOKEEPER_CONNECT);// 声明zk
		properties.put("group.id", ParamUtil.KAFKA_GROUP_ID);// 不同的group维护不同的offset
		properties.put("auto.commit.interval.ms", ParamUtil.KAFKA_AUTO_COMMIT_INTERVAL_MS);// 该参数尽量不小于写满WRITEBUFFERSIZE所需的时间
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}
}
