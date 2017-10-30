package com.asiainfo.Main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.HbaseDao.HbaseDao;
import com.asiainfo.HbaseDao.HbaseDaoThread;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.KafkaDao.KafkaMyConsumer;


public class HbaseMain {
    public  static final Lock lock  = new ReentrantLock();
    public  static final Condition notFull = lock.newCondition();
    public  static final Condition notEmpty= lock.newCondition();
    
    public static long time;
    public static long time2;
    
	public static ArrayList<String[]>  dataList = new ArrayList<String[]>();
	
	public static HashMap<String, Integer> mma = new HashMap<String,Integer>();
	public static int batch = 40000;
	
	public final static String TOPIC_CS_LOC_SIGNAL = "LOC_MSG_SIGNAL_KAFUKA";
	public final static String TOPIC_CS_SMS_SIGNAL = "SMS_MSG_SIGNAL_KAFKA";
	public final static String TOPIC_CS_VOC_SIGNAL = "VOC_MSG_SIGNAL_KAFKA";
	public final static String TOPIC_PS_SIGNAL = "PS_SIGNAL_INPUT_KAFKA_SRC";
	public final static String TOPIC_PS_ROAM_SIGNAL = "PS_ROAM_SIGNAL_INPUT_KAFKA_SRC";

	public static void main(String[] args) throws Exception {
		if (args.length == 1 && args[0].equals("test")) {
			new HbaseMainNew().doTest();
//			System.out.println();
//			List<String> lacciList = new ArrayList<String>();
//			lacciList.add("55233|1221");
//			lacciList.add("55233|1222");
//			Map<String, Long> SectionResult = new HbaseDao().getSizeBatch(HbaseDao.TABLE_NAME_INDEX, lacciList);
//			for(String key:SectionResult.keySet()){
//				System.out.println(key+" "+SectionResult.get(key));
//			}
		} else if (args.length == 3 && args[0].equals("input")) {
				batch = Integer.parseInt(args[1]);
				System.out.println("consumer start input to hbase!");
				
				HbaseDaoThread.taskSize=Integer.parseInt(args[2]);
				
				System.out.println("taskSize "+HbaseDaoThread.taskSize);
				
				KafkaMyConsumer kafkaMyConsumerPS = new KafkaMyConsumer(TOPIC_PS_SIGNAL);
				kafkaMyConsumerPS.setPriority(10);
				
		        Runtime.getRuntime().addShutdownHook(new Thread() {
		            @Override
		            public void run() {
		            	System.out.println("ShutdownHook");
		            	new HbaseInput().HbaseSignalPut(HbaseMain.dataList);
		            }
		        });
		        
				new KafkaMyConsumer(TOPIC_CS_LOC_SIGNAL).start();
				new KafkaMyConsumer(TOPIC_CS_SMS_SIGNAL).start();
				new KafkaMyConsumer(TOPIC_CS_VOC_SIGNAL).start();
				new KafkaMyConsumer(TOPIC_PS_ROAM_SIGNAL).start();
				
				kafkaMyConsumerPS.start();
				
				time=System.currentTimeMillis();
				time2=System.currentTimeMillis();
				
				new DataConsumer().start();
			
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

class DataConsumer extends Thread{
    DataConsumer(){
    }
    @Override
    public void run() {
        while(true){
            new HbaseInput().take();
        }
    }   
}

