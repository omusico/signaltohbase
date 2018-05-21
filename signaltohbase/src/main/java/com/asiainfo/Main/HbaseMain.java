package com.asiainfo.Main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.asiainfo.KafkaDao.KafkaOutput;
import com.asiainfo.KafkaDao.KafkaSignaConsumerZbRoamput;
import com.asiainfo.KafkaDao.KafkaSignalConsumer;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsLoc;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsMap;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsSms;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsVoc;
import com.asiainfo.KafkaDao.KafkaSignalConsumerPs;
import com.asiainfo.KafkaDao.KafkaSignalConsumerPsRoam;
import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.ParamUtil;


public class HbaseMain {
	private static Logger logger = Log4JUtil.getLogger();
	
    public  static final Lock lock  = new ReentrantLock();
    public  static final Condition notFull = lock.newCondition();
    public  static final Condition notEmpty= lock.newCondition();
    
	public static HashMap<String, Integer> mma = new HashMap<String,Integer>();
	
	public static List<Long> timeList = new ArrayList<Long>();
	public static Map<String, String> timeMap = new LinkedHashMap<String,String>();
	
//	public static List<String> signalFormatList = new ArrayList<String>();
	
	public static String[] argString ;

	public static void main(String[] args) throws Exception {
		ParamUtil.getInstance();
		argString = args;
		new HbaseMain().ExeMain(argString);
	}
	
	public void ExeMain(String[] argString){
		logger.info("consumer start input to hbase!");
		Log4JUtil.addNowts();
		
		String signal = argString[0];
		try {
			KafkaSignalConsumer.InitKafkaOutput((KafkaOutput)Class.forName(ParamUtil.KAFKAOUTPUTCLASS).newInstance());
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		

		
		switch (signal) {
		case "csloc":
			new KafkaSignalConsumerCsLoc (ParamUtil.TOPIC_CS_LOC_SIGNAL, 2).start();
			break;
		case "csvoc":
			new KafkaSignalConsumerCsVoc (ParamUtil.TOPIC_CS_VOC_SIGNAL, 2).start();
			break;
		case "cssms":
			new KafkaSignalConsumerCsSms (ParamUtil.TOPIC_CS_SMS_SIGNAL, 2).start();
			break;
		case "csmap":
			new KafkaSignalConsumerCsMap (ParamUtil.TOPIC_MAP_DEPOSIT_SIGNAL,2).start();
			break;
		case "ps":
			new KafkaSignalConsumerPs    (ParamUtil.TOPIC_PS_SIGNAL, 6).start();
			break;
		case "psroam":
			Log4JUtil.LOGGER4JFILE = "psroamlog4j.properties";
			new KafkaSignalConsumerPsRoam(ParamUtil.TOPIC_PS_ROAM_SIGNAL,6).start();
			break;
		case "zbroamout":
			Log4JUtil.LOGGER4JFILE = "zbroamoutlog4j.properties";
			new KafkaSignaConsumerZbRoamput(ParamUtil.TOPIC_ZB_ROAMOUT_SIGNAL,2).start();
			break;
		case "all":
			new KafkaSignalConsumerCsLoc (ParamUtil.TOPIC_CS_LOC_SIGNAL, 2).start();
			new KafkaSignalConsumerCsVoc (ParamUtil.TOPIC_CS_VOC_SIGNAL, 2).start();
			new KafkaSignalConsumerCsSms (ParamUtil.TOPIC_CS_SMS_SIGNAL, 2).start();
			new KafkaSignalConsumerPs    (ParamUtil.TOPIC_PS_SIGNAL, 6).start();
//			new KafkaSignalConsumerCsMap (ParamUtil.TOPIC_MAP_DEPOSIT_SIGNAL,2).start();
//			new KafkaSignalConsumerPsRoam(ParamUtil.TOPIC_PS_ROAM_SIGNAL,6).start();
			break;
		default:
			break;
		} 
		new DataConsumer(KafkaOutput.signalFormatList).start();
		
//		
//		
//		if (argString.length == 1 && argString[0].equals("mutate")) {
//			new HbaseDao().Mutate();
//		}else
//		if (argString.length == 1 && argString[0].equals("test")) {
//			new HbaseMainTest().doTest();
//		}else
//		if (argString.length == 2 && argString[0].equals("testkafka")){
//			new KafkaMyConsumerOld(args[1], 2).start(); 
//		}else
//		if (argString.length == 1 && argString[0].equals("testparm")){
//			System.out.println(ParamUtil.kafka_group_id);
//		}else	
//		if (argString.length == 1 && argString[0].equals("input")) {
//			
//	        Runtime.getRuntime().addShutdownHook(new Thread() {
//	            @Override
//	            public void run() {
//	            	logger.info("ShutdownHook");
//	            	try {
//						new HbaseInput().HbaseSignalPut(HbaseInput.signalBeanList);
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//	            }
//	        });
//			
//			timeList.add(System.currentTimeMillis());
//	        
//			KafkaMyConsumerOld kafkaMyConsumerPS = new KafkaMyConsumerOld(paramUtil.TOPIC_PS_SIGNAL, 6);
//			kafkaMyConsumerPS.setPriority(10);
//			
//			new KafkaMyConsumerOld(paramUtil.TOPIC_CS_LOC_SIGNAL, 2).start(); 
//			new KafkaMyConsumerOld(paramUtil.TOPIC_CS_SMS_SIGNAL, 2).start();
//			new KafkaMyConsumerOld(paramUtil.TOPIC_CS_VOC_SIGNAL, 2).start();
////			new KafkaMyConsumerOld(paramUtil.TOPIC_PS_ROAM_SIGNAL,6).start();
////			new KafkaMyConsumerOld(paramUtil.TOPIC_MAP_DEPOSIT_SIGNAL,2).start();
////			new KafkaMyConsumerOld(paramUtil.TOPIC_ZB_ROAMOUT_SIGNAL,2).start();
//			
//			kafkaMyConsumerPS.start();
//
//			new DataConsumer().start();
//			
//		}else if (args.length >=2 && args[0].equals("output")) {
//			List<String> paras = new ArrayList<String>();
//			for(int i=2;i<args.length;i++){
//				paras.add(args[i]);
//			}
//			if(args[1].equals("sfz")){
//				KvImportMain_sfz.exemain(paras);
//			}else if (args[1].equals("jzh")) {
//				KvImportMain_jzh.exemain(paras);
//			}else if (args[1].equals("zfw")) {
//				KvImportMain_zfw.exemain(paras);
//			}else if (args[1].equals("jzhtest")) {
//				KvImportMain_jzhtest.exemain(paras);
//			}
//		}
	}
}
    

class DataConsumer extends Thread{
	private List<String> signalFormatList;
	DataConsumer(List<String> signalFormatList){
		this.signalFormatList = signalFormatList;
    }
    @Override
    public void run() {
        while(true){
            new KafkaOutput().readyForInput(signalFormatList);
        }
    }  
}

