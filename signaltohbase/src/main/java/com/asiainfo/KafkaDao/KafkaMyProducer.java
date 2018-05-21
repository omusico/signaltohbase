package com.asiainfo.KafkaDao;

import java.util.*;

import org.apache.kafka.clients.producer.*;



public class KafkaMyProducer {
    private final Producer<String, String> producer;
    private final String topic;
    private final List<String> msgSet;
    
    public KafkaMyProducer(String topic, List<String> msgSet) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.1.32:6667,192.168.1.33:6667,192.168.1.34:6667,192.168.1.18:6667");//broker 集群地址
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key 序列号方式
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value 序列号方式
        
        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
        this.msgSet = msgSet;
    }
    
//    @Override
//    public void run() {
//        for(String msg:msgSet){
//		    System.out.println(msg);
//		    producer.send(new ProducerRecord<String, String>(this.topic,msg));
//		}
//    }
    
    public void setSend(){
        for(String msg:msgSet){
		    producer.send(new ProducerRecord<String, String>(this.topic,msg));
		}
	    System.out.println("setSend complete!");
    }


}
