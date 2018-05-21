package com.asiainfo.Main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.asiainfo.HbaseDao.HbaseDao;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.KafkaDao.KafkaMyConsumerOld;
import com.asiainfo.KafkaDao.KafkaOutputCollection;
import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.ParamUtil;


public class HbaseMainOld {
	
//	private static Logger logger = Log4JUtil.getLogger(HbaseMain.class);
	
    public  static final Lock lock  = new ReentrantLock();
    public  static final Condition notFull = lock.newCondition();
    public  static final Condition notEmpty= lock.newCondition();
    
	public static HashMap<String, Integer> mma = new HashMap<String,Integer>();
	
	public static List<Long> timeList = new ArrayList<Long>();
	public static Map<String, String> timeMap = new LinkedHashMap<String,String>();
	
	
	public static String[] argString ;

	public static void main(String[] args) throws Exception {
//		logger.info("consumer start input to hbase!");
		
		ParamUtil paramUtil = ParamUtil.getInstance();

		argString = args;
		if (argString.length == 1 && argString[0].equals("mutate")) {
			new HbaseDao().Mutate();
		}else
		if (argString.length == 1 && argString[0].equals("test")) {
			new HbaseMainTest().doTest();
		}else
		if (argString.length == 2 && argString[0].equals("testkafka")){
			new KafkaMyConsumerOld(args[1], 2).start(); 
		}else
		if (argString.length == 1 && argString[0].equals("testparm")){
			System.out.println(paramUtil.KAFKA_GROUP_ID);
		}else	
		if (argString.length == 1 && argString[0].equals("input")) {
//			logger.info("consumer start input to hbase!");
			
	        Runtime.getRuntime().addShutdownHook(new Thread() {
	            @Override
	            public void run() {
	            	//TODO 判读是否需要SignalPut
//	            	logger.info("ShutdownHook");
	            	try {
						new HbaseInput().HbaseSignalPut(HbaseInput.signalBeanList);
					} catch (IOException e) {
						e.printStackTrace();
					}
	            }
	        });
			
			timeList.add(System.currentTimeMillis());
	        
			KafkaMyConsumerOld kafkaMyConsumerPS = new KafkaMyConsumerOld(paramUtil.TOPIC_PS_SIGNAL, 6);
			kafkaMyConsumerPS.setPriority(10);
			
			new KafkaMyConsumerOld(paramUtil.TOPIC_CS_LOC_SIGNAL, 2).start(); 
			new KafkaMyConsumerOld(paramUtil.TOPIC_CS_SMS_SIGNAL, 2).start();
			new KafkaMyConsumerOld(paramUtil.TOPIC_CS_VOC_SIGNAL, 2).start();
//			new KafkaMyConsumerOld(paramUtil.TOPIC_PS_ROAM_SIGNAL,6).start();
//			new KafkaMyConsumerOld(paramUtil.TOPIC_MAP_DEPOSIT_SIGNAL,2).start();
//			new KafkaMyConsumerOld(paramUtil.TOPIC_ZB_ROAMOUT_SIGNAL,2).start();
			
			kafkaMyConsumerPS.start();

//			new DataConsumer().start();
			
		}else if (args.length >=2 && args[0].equals("output")) {
			List<String> paras = new ArrayList<String>();
			for(int i=2;i<args.length;i++){
				paras.add(args[i]);
			}
			if(args[1].equals("sfz")){
				KvImportMain_sfz.exemain(paras);
			}else if (args[1].equals("jzh")) {
				KvImportMain_jzh.exemain(paras);
			}else if (args[1].equals("zfw")) {
				KvImportMain_zfw.exemain(paras);
			}else if (args[1].equals("jzhtest")) {
				KvImportMain_jzhtest.exemain(paras);
			}
		}
	}
}
//
//class DataConsumer extends Thread{
//    DataConsumer(){
//    }
//    @Override
//    public void run() {
//        while(true){
//            new HbaseInput().take();
//        }
//    }   
//}

