package com.asiainfo.KafkaDao;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author leicui bourne_cui@163.com
 */
public class KafkaMyProducer extends Thread {
	private final Producer<String, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public KafkaMyProducer(String topic) {
		props.put("zookeeper.connect", "132.151.46.79:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "132.151.46.79:9092");
		producer = new Producer<String, String>(new ProducerConfig(props));
		this.topic = topic;
	}

	@Override
	public void run() {
		for (int i = 1; i < 5; i++) {
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic, Integer.toString(i),
					i + ":" + i * 100);
			producer.send(km);
		}
	}
}
