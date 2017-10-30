package com.asiainfo.KafkaDao;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.Constants;
import com.asiainfo.Util.DataFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaMyConsumer extends Thread {
	private String topic;
	private static int partition_num;

	public KafkaMyConsumer(String topic){
		super();
		this.topic = topic;
		if(topic.equals(HbaseMain.TOPIC_PS_SIGNAL)){
			partition_num = 6;
		}else{
			partition_num = 2;
		}
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

				String[] upValue = SignalExe(new String(mam.message()));
				if(upValue!=null && !("").equals(upValue[0])  && !("").equals(upValue[1])){
					HbaseMain.lock.lock();
			        try {	
							while(HbaseMain.dataList.size() >= HbaseMain.batch){
				                HbaseMain.notEmpty.signalAll();//唤醒消费线程
				                HbaseMain.notFull.await();//阻塞生产线程
							}
							HbaseMain.dataList.add(upValue);    
			        }  catch (InterruptedException e) {
						e.printStackTrace();
					}finally{
				    	HbaseMain.lock.unlock();	
			        }
				}
					
					
//				String data = SignalExe(new String(mam.message()));	
//				if(!(data==null || data.equals("")||data.equals(",local")||data.equals(",roam"))){
//					HbaseMain.lock.lock();
//			        try {	
//							while(HbaseMain.dataList.size() >= HbaseMain.batch){
//				                HbaseMain.notEmpty.signalAll();//唤醒消费线程
//				                HbaseMain.notFull.await();//阻塞生产线程
//							}
//							HbaseMain.dataList.add(data);    
//			        }  catch (InterruptedException e) {
//						e.printStackTrace();
//					}finally{
//				    	HbaseMain.lock.unlock();	
//			        }
//				}
	        }
		}
	}
	
	private String[] SignalExe(String message) {
		DataFormat dataFormat = new DataFormat();
		if ((HbaseMain.TOPIC_CS_LOC_SIGNAL).equals(topic)) { 
			return dataFormat.CS_insert(Constants.LOCSIGNAL, message);
			
		} else if ((HbaseMain.TOPIC_CS_SMS_SIGNAL).equals(topic)) {
			return dataFormat.CS_insert(Constants.SMSSIGNAL, message);

		} else if ((HbaseMain.TOPIC_CS_VOC_SIGNAL).equals(topic)) {
			return dataFormat.CS_insert(Constants.VOCSIGNAL, message);

		} else if ((HbaseMain.TOPIC_PS_SIGNAL).equals(topic)) { 
			return dataFormat.PS_insert(message);
			
		} else if ((HbaseMain.TOPIC_PS_ROAM_SIGNAL).equals(topic)){
			return dataFormat.PS_roam_insert(message);
			
		}
		return null;
	}
	

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "192.168.2.48:2181,192.168.2.68:2181,192.168.1.14:2181");// 声明zk
		properties.put("group.id", "signal_hbase_test_7");// 不同的group维护不同的offset
		properties.put("auto.commit.interval.ms", "1000");// 该参数尽量不小于写满WRITEBUFFERSIZE所需的时间
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}
}
