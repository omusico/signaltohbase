package com.asiainfo.KafkaDao;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.asiainfo.Bean.SignalBean;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.Main.HbaseMainOld;
import com.asiainfo.Util.Constants;
import com.asiainfo.Util.DataFormat;
import com.asiainfo.Util.ParamUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaMyConsumerOld extends Thread {
	private ParamUtil paramUtil = ParamUtil.getInstance();
	private String topic;
	private int partition_num;

	public KafkaMyConsumerOld(String topic ,int partition_num){
		super();
		this.topic = topic;
		this.partition_num = partition_num;
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
				SignalBean signalBean = new SignalBean();
				//将kafka取出的字符串格式化处理
				String signalFormatString =  signalFormat(new String(mam.message()));

		        if ((signalFormatString != null) && (!"".equals(signalFormatString))) {
		            if ((HbaseMainOld.argString.length == 2) && (HbaseMainOld.argString[0].equals("testkafka"))){
		                System.out.println(signalFormatString);
		            }else{
						signalBean.setKafkaValues(Arrays.asList(signalFormatString.split(",",-1)));					
						HbaseMainOld.lock.lock();
				        try {	
							while(HbaseInput.signalBeanList.size() >= paramUtil.INPUTBATCHSIZE){
				                HbaseMainOld.notEmpty.signalAll();//唤醒消费线程
				                HbaseMainOld.notFull.await();//阻塞生产线程
							}
							HbaseInput.signalBeanList.add(signalBean);
				        }  catch (InterruptedException e) {
							e.printStackTrace();
						}finally{
					    	HbaseMainOld.lock.unlock();	
				        }
		            }
		        }

	        }
		}
	}
	
	public String signalFormat(String message) {
		DataFormat dataFormat = new DataFormat();
		if ((paramUtil.TOPIC_CS_LOC_SIGNAL).equals(topic)) { 
			return dataFormat.CS_insert(Constants.LOCSIGNAL, message);
			
		} else if ((paramUtil.TOPIC_CS_SMS_SIGNAL).equals(topic)) {
			return dataFormat.CS_insert(Constants.SMSSIGNAL, message);

		} else if ((paramUtil.TOPIC_CS_VOC_SIGNAL).equals(topic)) {
			return dataFormat.CS_insert(Constants.VOCSIGNAL, message);

		} else if ((paramUtil.TOPIC_PS_SIGNAL).equals(topic)) { 
			return dataFormat.PS_insert(message);
			
		} else if ((paramUtil.TOPIC_PS_ROAM_SIGNAL).equals(topic)){
			return dataFormat.PS_roam_insert(message);	
			
		}else if ((paramUtil.TOPIC_MAP_DEPOSIT_SIGNAL).equals(topic)){
			return dataFormat.map_insert(message);
			
		}else if ((paramUtil.TOPIC_ZB_ROAMOUT_SIGNAL).equals(topic)){
			return dataFormat.zb_insert(message);
			
		}
		return null;
	}
	

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		ParamUtil paramUtil = ParamUtil.getInstance();
		properties.put("zookeeper.connect", paramUtil.KAFKA_ZOOKEEPER_CONNECT);// 声明zk
		properties.put("group.id", paramUtil.KAFKA_GROUP_ID);// 不同的group维护不同的offset
		properties.put("auto.commit.interval.ms", paramUtil.KAFKA_AUTO_COMMIT_INTERVAL_MS);// 该参数尽量不小于写满WRITEBUFFERSIZE所需的时间
		properties.put("auto.offset.reset", "smallest");// 声明zk
		
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}
}
