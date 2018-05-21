package com.asiainfo.Util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;



public class ParamUtil {
	private String basePath = System.getProperty("user.dir");

	//[start]
	public static Integer getMUTATEDEL_POOL_SIZE() {
		return MUTATEDEL_POOL_SIZE;
	}

	public static void setMUTATEDEL_POOL_SIZE(Integer mUTATEDEL_POOL_SIZE) {
		MUTATEDEL_POOL_SIZE = mUTATEDEL_POOL_SIZE;
	}

	public static Long getMUTATEDEL_WRITEBUFFERSIZE() {
		return MUTATEDEL_WRITEBUFFERSIZE;
	}

	public static void setMUTATEDEL_WRITEBUFFERSIZE(Long mUTATEDEL_WRITEBUFFERSIZE) {
		MUTATEDEL_WRITEBUFFERSIZE = mUTATEDEL_WRITEBUFFERSIZE;
	}

	public static Integer getMUTATEPUT_POOL_SIZE() {
		return MUTATEPUT_POOL_SIZE;
	}

	public static void setMUTATEPUT_POOL_SIZE(Integer mUTATEPUT_POOL_SIZE) {
		MUTATEPUT_POOL_SIZE = mUTATEPUT_POOL_SIZE;
	}

	public static Long getMUTATEPUT_WRITEBUFFERSIZE() {
		return MUTATEPUT_WRITEBUFFERSIZE;
	}

	public static void setMUTATEPUT_WRITEBUFFERSIZE(Long mUTATEPUT_WRITEBUFFERSIZE) {
		MUTATEPUT_WRITEBUFFERSIZE = mUTATEPUT_WRITEBUFFERSIZE;
	}

	public static Integer getGET_POOL_SIZE() {
		return GET_POOL_SIZE;
	}

	public static void setGET_POOL_SIZE(Integer gET_POOL_SIZE) {
		GET_POOL_SIZE = gET_POOL_SIZE;
	}

	public static Integer getDEFAULT_POOL_SIZE() {
		return DEFAULT_POOL_SIZE;
	}

	public static void setDEFAULT_POOL_SIZE(Integer dEFAULT_POOL_SIZE) {
		DEFAULT_POOL_SIZE = dEFAULT_POOL_SIZE;
	}

	public static String getHBASE_ZOOKEEPER_QUORUM() {
		return HBASE_ZOOKEEPER_QUORUM;
	}

	public static void setHBASE_ZOOKEEPER_QUORUM(String hBASE_ZOOKEEPER_QUORUM) {
		HBASE_ZOOKEEPER_QUORUM = hBASE_ZOOKEEPER_QUORUM;
	}

	public static String getZOOKEEPER_ZNODE_PARENT() {
		return ZOOKEEPER_ZNODE_PARENT;
	}

	public static void setZOOKEEPER_ZNODE_PARENT(String zOOKEEPER_ZNODE_PARENT) {
		ZOOKEEPER_ZNODE_PARENT = zOOKEEPER_ZNODE_PARENT;
	}

	public static String getTABLE_NAME() {
		return TABLE_NAME;
	}

	public static void setTABLE_NAME(String tABLE_NAME) {
		TABLE_NAME = tABLE_NAME;
	}

	public static String getTABLE_NAME_INDEX() {
		return TABLE_NAME_INDEX;
	}

	public static void setTABLE_NAME_INDEX(String tABLE_NAME_INDEX) {
		TABLE_NAME_INDEX = tABLE_NAME_INDEX;
	}

	public static String getTABLE_ROAM_NAME() {
		return TABLE_ROAM_NAME;
	}

	public static void setTABLE_ROAM_NAME(String tABLE_ROAM_NAME) {
		TABLE_ROAM_NAME = tABLE_ROAM_NAME;
	}

	public static String getTABLE_ROAM_NAME_INDEX() {
		return TABLE_ROAM_NAME_INDEX;
	}

	public static void setTABLE_ROAM_NAME_INDEX(String tABLE_ROAM_NAME_INDEX) {
		TABLE_ROAM_NAME_INDEX = tABLE_ROAM_NAME_INDEX;
	}

	public static String getKAFKA_ZOOKEEPER_CONNECT() {
		return KAFKA_ZOOKEEPER_CONNECT;
	}

	public static void setKAFKA_ZOOKEEPER_CONNECT(String kAFKA_ZOOKEEPER_CONNECT) {
		KAFKA_ZOOKEEPER_CONNECT = kAFKA_ZOOKEEPER_CONNECT;
	}

	public static String getKAFKA_GROUP_ID() {
		return KAFKA_GROUP_ID;
	}

	public static void setKAFKA_GROUP_ID(String kAFKA_GROUP_ID) {
		KAFKA_GROUP_ID = kAFKA_GROUP_ID;
	}

	public static String getKAFKA_AUTO_COMMIT_INTERVAL_MS() {
		return KAFKA_AUTO_COMMIT_INTERVAL_MS;
	}

	public static void setKAFKA_AUTO_COMMIT_INTERVAL_MS(String kAFKA_AUTO_COMMIT_INTERVAL_MS) {
		KAFKA_AUTO_COMMIT_INTERVAL_MS = kAFKA_AUTO_COMMIT_INTERVAL_MS;
	}

	public static String getTOPIC_CS_LOC_SIGNAL() {
		return TOPIC_CS_LOC_SIGNAL;
	}

	public static void setTOPIC_CS_LOC_SIGNAL(String tOPIC_CS_LOC_SIGNAL) {
		TOPIC_CS_LOC_SIGNAL = tOPIC_CS_LOC_SIGNAL;
	}

	public static String getTOPIC_CS_SMS_SIGNAL() {
		return TOPIC_CS_SMS_SIGNAL;
	}

	public static void setTOPIC_CS_SMS_SIGNAL(String tOPIC_CS_SMS_SIGNAL) {
		TOPIC_CS_SMS_SIGNAL = tOPIC_CS_SMS_SIGNAL;
	}

	public static String getTOPIC_CS_VOC_SIGNAL() {
		return TOPIC_CS_VOC_SIGNAL;
	}

	public static void setTOPIC_CS_VOC_SIGNAL(String tOPIC_CS_VOC_SIGNAL) {
		TOPIC_CS_VOC_SIGNAL = tOPIC_CS_VOC_SIGNAL;
	}

	public static String getTOPIC_PS_SIGNAL() {
		return TOPIC_PS_SIGNAL;
	}

	public static void setTOPIC_PS_SIGNAL(String tOPIC_PS_SIGNAL) {
		TOPIC_PS_SIGNAL = tOPIC_PS_SIGNAL;
	}

	public static String getTOPIC_PS_ROAM_SIGNAL() {
		return TOPIC_PS_ROAM_SIGNAL;
	}

	public static void setTOPIC_PS_ROAM_SIGNAL(String tOPIC_PS_ROAM_SIGNAL) {
		TOPIC_PS_ROAM_SIGNAL = tOPIC_PS_ROAM_SIGNAL;
	}

	public static String getTOPIC_MAP_DEPOSIT_SIGNAL() {
		return TOPIC_MAP_DEPOSIT_SIGNAL;
	}

	public static void setTOPIC_MAP_DEPOSIT_SIGNAL(String tOPIC_MAP_DEPOSIT_SIGNAL) {
		TOPIC_MAP_DEPOSIT_SIGNAL = tOPIC_MAP_DEPOSIT_SIGNAL;
	}

	public static String getTOPIC_ZB_ROAMOUT_SIGNAL() {
		return TOPIC_ZB_ROAMOUT_SIGNAL;
	}

	public static void setTOPIC_ZB_ROAMOUT_SIGNAL(String tOPIC_ZB_ROAMOUT_SIGNAL) {
		TOPIC_ZB_ROAMOUT_SIGNAL = tOPIC_ZB_ROAMOUT_SIGNAL;
	}

	public static Integer getUPVALUE_POOL_SIZE() {
		return UPVALUE_POOL_SIZE;
	}

	public static void setUPVALUE_POOL_SIZE(Integer uPVALUE_POOL_SIZE) {
		UPVALUE_POOL_SIZE = uPVALUE_POOL_SIZE;
	}

	public static String getISMUTATE() {
		return ISMUTATE;
	}

	public static void setISMUTATE(String iSMUTATE) {
		ISMUTATE = iSMUTATE;
	}

	public static String getISDUPLGET() {
		return ISDUPLGET;
	}

	public static void setISDUPLGET(String iSDUPLGET) {
		ISDUPLGET = iSDUPLGET;
	}

	public static Integer getINPUTBATCHSIZE() {
		return INPUTBATCHSIZE;
	}

	public static void setINPUTBATCHSIZE(Integer iNPUTBATCHSIZE) {
		INPUTBATCHSIZE = iNPUTBATCHSIZE;
	}

	public static Integer getSLEEPSEC() {
		return SLEEPSEC;
	}

	public static void setSLEEPSEC(Integer sLEEPSEC) {
		SLEEPSEC = sLEEPSEC;
	}

	public static Integer getLOGBATCHSIZE() {
		return LOGBATCHSIZE;
	}

	public static void setLOGBATCHSIZE(Integer lOGBATCHSIZE) {
		LOGBATCHSIZE = lOGBATCHSIZE;
	}

	public static String getKAFKAOUTPUTCLASS() {
		return KAFKAOUTPUTCLASS;
	}

	public static void setKAFKAOUTPUTCLASS(String kAFKAOUTPUTCLASS) {
		KAFKAOUTPUTCLASS = kAFKAOUTPUTCLASS;
	}
	//[end]

	public static Integer MUTATEDEL_POOL_SIZE = 1;
	public static Long    MUTATEDEL_WRITEBUFFERSIZE = 3145728L;
	public static Integer MUTATEPUT_POOL_SIZE = 1;
	public static Long    MUTATEPUT_WRITEBUFFERSIZE = 3145728L;
	public static Integer GET_POOL_SIZE = 1;
	public static Integer DEFAULT_POOL_SIZE = 10; 
	
	//HbasePool-conf
	public static String HBASE_ZOOKEEPER_QUORUM = "ocdc-dn-03,ocdc-dn-22,ocdc-ser-01";
	public static String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";
	
	//Hbase-tablename
	public static String TABLE_NAME = "ZJLT:signal";
	public static String TABLE_NAME_INDEX = "ZJLT:signalindex";
	public static String TABLE_ROAM_NAME = "ZJLT:roamsignal";
	public static String TABLE_ROAM_NAME_INDEX = "ZJLT:roamsignalindex";
	
	//kafka-conf
	public static String KAFKA_ZOOKEEPER_CONNECT = "192.168.2.48:2181,192.168.2.68:2181,192.168.1.14:2181";
	public static String KAFKA_GROUP_ID = "signal_hbase_1";
	public static String KAFKA_AUTO_COMMIT_INTERVAL_MS = "1000";
	
	public static String TOPIC_CS_LOC_SIGNAL  = "LOC_MSG_SIGNAL_KAFUKA";
	public static String TOPIC_CS_SMS_SIGNAL  = "SMS_MSG_SIGNAL_KAFKA";
	public static String TOPIC_CS_VOC_SIGNAL  = "VOC_MSG_SIGNAL_KAFKA";
	public static String TOPIC_PS_SIGNAL      = "PS_SIGNAL_INPUT_KAFKA_SRC";
	public static String TOPIC_PS_ROAM_SIGNAL = "PS_ROAM_SIGNAL_INPUT_KAFKA_SRC";
	public static String TOPIC_MAP_DEPOSIT_SIGNAL   = "MAP_DEPOSIT_SIGNAL";
	public static String TOPIC_ZB_ROAMOUT_SIGNAL   = "zb_roam_out";
	
	public static Integer UPVALUE_POOL_SIZE = 1;
	
	//code-if
	public static String ISMUTATE ="0";
	public static String ISDUPLGET="1";
	
	public static Integer INPUTBATCHSIZE = 200000;
	
	public static Integer SLEEPSEC = 10;

	public static Integer LOGBATCHSIZE = 10000;
	
	/*	KafkaOutput的实现类*/
	public static String KAFKAOUTPUTCLASS = "com.asiainfo.KafkaDao.KafkaOutputConsole";
	
	private static class SingletonParamUtil{
		private static ParamUtil paramUtil = new ParamUtil();
	}
	
	public static ParamUtil getInstance(){
		ParamUtil paramUtil = SingletonParamUtil.paramUtil;
		paramUtil.init();
		return paramUtil;
	}
	
	public void init(){
		Properties props=null;
		InputStream in=null;
        props = new Properties();
	    try
	    {
			File file = new File(basePath+"/conf/config.conf");
	        in = new BufferedInputStream (new FileInputStream(file));
	        props.load(in);
	    }catch(Exception e){
	    	System.err.println("未找到配置文件 "+basePath+"/conf/config.conf"+", 使用jar包内置参数");
	    	e.printStackTrace();
	    }

		try {
			Class<?> clazz = Class.forName("com.asiainfo.Util.ParamUtil"); 
			Field[] fields = clazz.getFields();
			for(Field field : fields){

				if(props.getProperty(field.getName())!=null){
					Method mSet = clazz.getDeclaredMethod("set"+field.getName(),field.getType());

					if(field.getType().isInstance("java.lang.String")){
						mSet.invoke(clazz, props.getProperty(field.getName()));
					}else{
			    		Method methodValueOf = field.getType().getMethod("valueOf", String.class);
			    		mSet.invoke(clazz, methodValueOf.invoke(field.getClass(), props.getProperty(field.getName())));
					}
				}

			}
		} catch (ClassNotFoundException | SecurityException | NoSuchMethodException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
			
	}


	
//	public void init(){
//		
//		 File file = new File(basePath+"/conf/config.conf");
//		 Properties props=null;
//		 InputStream in=null;
//	        try
//	        {
//	            in = new BufferedInputStream (new FileInputStream(file));
//	            props = new Properties();
//	            props.load(in);
//	            
//	            
//	            if((props.getProperty("MutateDel_WriteBufferSize"))!=null){
//	            	MutateDel_WriteBufferSize = Long.valueOf(props.getProperty("MutateDel_WriteBufferSize"));
//	            }
//	            if((props.getProperty("MutateDel_POOL_SIZE"))!=null){
//	            	MutateDel_POOL_SIZE = Integer.valueOf(props.getProperty("MutateDel_POOL_SIZE"));
//	            }	            
//	            if((props.getProperty("MutatePut_WriteBufferSize"))!=null){
//	            	MutatePut_WriteBufferSize = Long.valueOf(props.getProperty("MutatePut_WriteBufferSize"));
//	            }
//	            if((props.getProperty("MutatePut_POOL_SIZE"))!=null){
//	            	MutatePut_POOL_SIZE = Integer.valueOf(props.getProperty("MutatePut_POOL_SIZE"));
//	            }
//	            if((props.getProperty("Get_POOL_SIZE"))!=null){
//	            	Get_POOL_SIZE = Integer.valueOf(props.getProperty("Get_POOL_SIZE"));
//	            }
//	            if((props.getProperty("Default_POOL_SIZE"))!=null){
//	            	Default_POOL_SIZE = Integer.valueOf(props.getProperty("Default_POOL_SIZE"));
//	            }	          	            
//	            if((props.getProperty("hbase_zookeeper_quorum"))!=null){
//	            	hbase_zookeeper_quorum = props.getProperty("hbase_zookeeper_quorum");
//	            }
//	            if((props.getProperty("zookeeper_znode_parent"))!=null){
//	            	zookeeper_znode_parent = props.getProperty("zookeeper_znode_parent");
//	            }
//	            if((props.getProperty("TABLE_NAME"))!=null){
//	            	TABLE_NAME = props.getProperty("TABLE_NAME");
//	            }
//	            if((props.getProperty("TABLE_NAME_INDEX"))!=null){
//	            	TABLE_NAME_INDEX = props.getProperty("TABLE_NAME_INDEX");
//	            }
//	            if((props.getProperty("TABLE_ROAM_NAME"))!=null){
//	            	TABLE_ROAM_NAME = props.getProperty("TABLE_ROAM_NAME");
//	            }
//	            if((props.getProperty("TABLE_ROAM_NAME_INDEX"))!=null){
//	            	TABLE_ROAM_NAME_INDEX = props.getProperty("TABLE_ROAM_NAME_INDEX");
//	            }
//	            if((props.getProperty("kafka_zookeeper_connect"))!=null){
//	            	kafka_zookeeper_connect = props.getProperty("kafka_zookeeper_connect");
//	            }
//	            if((props.getProperty("kafka_group_id"))!=null){
//	            	kafka_group_id = props.getProperty("kafka_group_id");
//	            }
//	            if((props.getProperty("kafka_auto_commit_interval_ms"))!=null){
//	            	kafka_auto_commit_interval_ms = props.getProperty("kafka_auto_commit_interval_ms");
//	            }
//	            if((props.getProperty("isMutate"))!=null){
//	            	isMutate = props.getProperty("isMutate");
//	            }
//	            if((props.getProperty("isDuplGet"))!=null){
//	            	isDuplGet = props.getProperty("isDuplGet");
//	            }
//	            if((props.getProperty("UPVALUE_POOL_SIZE"))!=null){
//	            	UPVALUE_POOL_SIZE = Integer.valueOf(props.getProperty("UPVALUE_POOL_SIZE"));
//	            }
//	            if((props.getProperty("size_perbatch"))!=null){
//	            	size_perbatch = Integer.valueOf(props.getProperty("size_perbatch"));
//	            }	
//	            if((props.getProperty("sleepSec"))!=null){
//	            	sleepSec = Integer.valueOf(props.getProperty("sleepSec"));
//	            }	
//	            if((props.getProperty("TOPIC_CS_LOC_SIGNAL"))!=null){
//	            	TOPIC_CS_LOC_SIGNAL = props.getProperty("TOPIC_CS_LOC_SIGNAL");
//	            }	
//	            if((props.getProperty("TOPIC_CS_SMS_SIGNAL"))!=null){
//	            	TOPIC_CS_SMS_SIGNAL = props.getProperty("TOPIC_CS_SMS_SIGNAL");
//	            }	
//	            if((props.getProperty("TOPIC_CS_VOC_SIGNAL"))!=null){
//	            	TOPIC_CS_VOC_SIGNAL = props.getProperty("TOPIC_CS_VOC_SIGNAL");
//	            }	
//	            if((props.getProperty("TOPIC_PS_SIGNAL"))!=null){
//	            	TOPIC_PS_SIGNAL = props.getProperty("TOPIC_PS_SIGNAL");
//	            }	
//	            if((props.getProperty("TOPIC_PS_ROAM_SIGNAL"))!=null){
//	            	TOPIC_PS_ROAM_SIGNAL = props.getProperty("TOPIC_PS_ROAM_SIGNAL");
//	            }	
//	            if((props.getProperty("TOPIC_MAP_DEPOSIT_SIGNAL"))!=null){
//	            	TOPIC_MAP_DEPOSIT_SIGNAL = props.getProperty("TOPIC_MAP_DEPOSIT_SIGNAL");
//	            }	
//	            if((props.getProperty("TOPIC_ZB_ROAMOUT_SIGNAL"))!=null){
//	            	TOPIC_ZB_ROAMOUT_SIGNAL = props.getProperty("TOPIC_ZB_ROAMOUT_SIGNAL");
//	            }	
//   
//	        }catch(Exception e){
//	        	System.out.println("未找到配置文件 "+basePath+"/conf/config.conf"+", 使用jar包内置参数" );
//	        }finally{
//	        	try {
//	        		if(in!=null){
//	        			in.close();
//	        		}
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//	        	
//	        }
//	}
	
	public static void main(String[] args) {
		ParamUtil.getInstance();
		System.out.println(ParamUtil.MUTATEDEL_WRITEBUFFERSIZE+" "
				+ParamUtil.KAFKA_GROUP_ID+" "
				+ParamUtil.TABLE_NAME);
	}
}
