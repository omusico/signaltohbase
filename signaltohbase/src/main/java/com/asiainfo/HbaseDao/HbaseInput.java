package com.asiainfo.HbaseDao;


import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.asiainfo.Bean.SignalBean;
import com.asiainfo.Main.HbaseMainOld;
import com.asiainfo.Util.DataFormat;
import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.ParamUtil;
import com.asiainfo.Util.TimeFormat;



public class HbaseInput{
	public static List<SignalBean>  signalBeanList = new ArrayList<SignalBean>();
	Logger logger = Log4JUtil.getLogger();
	
	ParamUtil paramUtil = ParamUtil.getInstance();
	public final static String STRING_f = "f";
	public final static String STRING_rts = "rts";
	public final static String STRING_rtrs = "rtrs";
	public final static String STRING_empty = "";
	public final static String STRING_laccisplit = "|";
	public final static byte[] BYTE_f = STRING_f.getBytes();
	public final static byte[] BYTE_rts = STRING_rts.getBytes();
	public final static byte[] BYTE_rtrs = STRING_rtrs.getBytes();
	public final static byte[] BYTE_empty = "".getBytes();
	
	public static String mdn            = "mdn";    
	public static String signalType     = "signalType";   
	public static String area           = "area";
	public static String roam           = "roam";
	public static String roamProv       = "roamProv";
	public static String roamCountry    = "roamCountry";
	public static String lac            = "lac";
	public static String ci             = "ci";
	public static String longitude      = "longitude";
	public static String latitude       = "latitude";
	public static String imsi           = "imsi";
	public static String imei           = "imei";
	public static String currentCiTime  = "currentCiTime";
	public static String lastTime       = "lastTime";
	
	public final static byte[] bytes_mdn           = mdn.getBytes();
	public final static byte[] bytes_signalType    = signalType.getBytes();
	public final static byte[] bytes_area          = area.getBytes();
	public final static byte[] bytes_roam          = roam.getBytes();
	public final static byte[] bytes_roamProv      = roamProv.getBytes();
	public final static byte[] bytes_roamCountry   = roamCountry.getBytes();
	public final static byte[] bytes_lac           = lac.getBytes();
	public final static byte[] bytes_ci            = ci.getBytes();
	public final static byte[] bytes_longitude     = longitude.getBytes();
	public final static byte[] bytes_latitude      = latitude.getBytes();
	public final static byte[] bytes_imsi          = imsi.getBytes();
	public final static byte[] bytes_imei          = imei.getBytes();
	public final static byte[] bytes_currentCiTime = currentCiTime.getBytes();
	public final static byte[] bytes_lastTime      = lastTime.getBytes();
	
    public void take() {  
        HbaseMainOld.lock.lock();
        try {  
            while (signalBeanList.size() >= ParamUtil.INPUTBATCHSIZE ){//如果队列满了
                      
            	HbaseMainOld.timeList.add(System.currentTimeMillis());
            	HbaseMainOld.timeMap.put("10000 batch: ", ""+
            			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1)- HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-2))
            			/signalBeanList.size());
            	
            	try {
					HbaseSignalPut(signalBeanList);
				} catch (IOException e) {
					e.printStackTrace();
				}
            	
      
            	HbaseMainOld.timeList.add(System.currentTimeMillis());     
            	HbaseMainOld.timeMap.put("10000 all: ", ""+
            			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1)- HbaseMainOld.timeList.get(0))
            			/ParamUtil.INPUTBATCHSIZE);
            	
            	Log4JUtil.getTimeLog();
            	
            	
            	HbaseMainOld.notFull.signalAll();//唤醒生产线程
            }
            HbaseMainOld.notEmpty.await();//阻塞消费线程
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
        	HbaseMainOld.lock.unlock();
        }
    }
    
    
	public void HbaseSignalPut(List<SignalBean> signalBeanList) throws IOException {
		ParamUtil paramUtil = ParamUtil.getInstance();

		DataFormat dataFormat = new DataFormat();
		HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();
		
		HbaseMainOld.timeList.add(System.currentTimeMillis());
    	//kafka signal data remove dupl
    	Map<String, SignalBean> SignalBeanMap = new ConcurrentHashMap<String, SignalBean>();
    	for(SignalBean signalBean:signalBeanList){
			String Mdn = signalBean.getKafkaValues().get(0);
			if(SignalBeanMap.containsKey(Mdn)){
				//这个get就好像是从hbase get到的，所以是hbaseSignalBean
				SignalBean duplSignalBean = SignalBeanMap.get(Mdn);
				signalBean.setHbaseValues(duplSignalBean.getKafkaValues());
				duplSignalBean = dataFormat.compareSignalBean(signalBean);
				duplSignalBean.setHbaseValues(null);
				SignalBeanMap.put(Mdn, duplSignalBean);
			}else{
				SignalBeanMap.put(Mdn, signalBean);
			}
		}
    	
//    	System.out.println("++++++compare++++++");
//    	for(Entry<String, SignalBean>  entry:SignalBeanMap.entrySet()){
//    		System.out.println(entry.getValue());
//    	}
		
		HbaseMainOld.timeList.add(System.currentTimeMillis());
    	HbaseMainOld.timeMap.put("10000 dupl: ", ""+
    			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1) - HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-2))
    			/signalBeanList.size());
		
    	
    	Set<SignalBean> signalBeanSet = new HashSet<SignalBean>();
    	for(Entry<String, SignalBean> entry: SignalBeanMap.entrySet()){
    		signalBeanSet.add(entry.getValue());
    	}
    	
		//hbase thread get 
    	Set<SignalBean> hbaseSignalBeanMap = hbaseDaoThread.HbaseGetternew(paramUtil.TABLE_NAME, signalBeanSet);

//    	System.out.println("++++++get++++++");
//    	for( SignalBean  signalBean:hbaseSignalBeanMap){
//    		System.out.println(signalBean);
//    	}
    	
		//time  
		HbaseMainOld.timeList.add(System.currentTimeMillis());
    	HbaseMainOld.timeMap.put("10000 get: ", ""+
    			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1)- HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-2))
    			/signalBeanList.size());
    	
    	
    	Set<SignalBean> resultSignalBeanSet = hbaseDaoThread.HbaseUpValue(hbaseSignalBeanMap);
  	
//    	System.out.println("++++++upvalue++++++");
//    	for( SignalBean  signalBean:resultSignalBeanSet){
//    		System.out.println(signalBean);
//    	}
    	
		HbaseMainOld.timeList.add(System.currentTimeMillis());
    	HbaseMainOld.timeMap.put("10000 finvalue: ", ""+
    			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1)- HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-2))
    			/signalBeanList.size() );


//    	List<IndexBean> allDelIndexBeanList = new ArrayList<IndexBean>();
//    	List<IndexBean> allInsIndexBeanList = new ArrayList<IndexBean>();
//    	for(SignalBean signalBean : resultSignalBeanSet){
//    		List<IndexBean> delIndexBeanList = signalBean.getDelIndexBeanList();
//    		for(IndexBean delIndexBean : delIndexBeanList){
//    			if(paramUtil.TABLE_NAME_INDEX.equals(delIndexBean.getTable())){
//    				allDelIndexBeanList.add(delIndexBean);
//    			}    			
//    		}
//    		List<IndexBean> insIndexBeanList = signalBean.getInsIndexBeanList();
//    		for(IndexBean insIndexBean : insIndexBeanList){
//    			if(paramUtil.TABLE_NAME_INDEX.equals(insIndexBean.getTable())){
//    				allInsIndexBeanList.add(insIndexBean);
//    			}    			
//    		}
//    	}
//    	
////    	hbaseDaoThread.HbaseMutatorDel(ParamUtil.TABLE_NAME,HbaseInput.BYTE_f,LocalDelMap);
    	hbaseDaoThread.HbaseMutatorDel(ParamUtil.TABLE_NAME_INDEX , resultSignalBeanSet);
////    	hbaseDaoThread.HbaseMutatorDel(ParamUtil.TABLE_ROAM_NAME_INDEX,HbaseInput.BYTE_f,RoamIndexDelMap);
//    	
		HbaseMainOld.timeList.add(System.currentTimeMillis());
    	HbaseMainOld.timeMap.put("10000 delete: ",""+ 
    			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1)- HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-2))
    			/signalBeanList.size() );
//    	
//
//    	
    	if(paramUtil.ISMUTATE.equals("0")){
    		System.out.println("NotMutate");
    		hbaseDaoThread.HbasePutter(resultSignalBeanSet);
//          hbaseDaoThread.HbasePutter(ParamUtil.TABLE_NAME, HbaseInput.STRING_rtrs, ParamUtil.TABLE_ROAM_NAME_INDEX, finRoamDataMap);
     
    	}else if(paramUtil.ISMUTATE.equals("1")){
    		System.out.println("IsMutate");
    		hbaseDaoThread.HbaseMutatorPut(resultSignalBeanSet);
//        	hbaseDaoThread.HbaseMutatorPut(paramUtil.TABLE_NAME_INDEX, allInsIndexBeanList);
    	}
    	else if(paramUtil.ISMUTATE.equals("2")){
//    		System.out.println("IsMutate");
//    		hbaseDaoThread.HbaseMutatorPut2(resultSignalBeanSet);
//        	hbaseDaoThread.HbaseMutatorPut(paramUtil.TABLE_NAME_INDEX, allInsIndexBeanList);
    	}
//   	
//
//		
    	HbaseMainOld.timeList.add(System.currentTimeMillis());
    	HbaseMainOld.timeMap.put("10000 put: ", ""+
    			10000 *(HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-1)- HbaseMainOld.timeList.get(HbaseMainOld.timeList.size()-2))
    			/signalBeanList.size() );
//    	HbaseMain.timeMap.put("put ops: ", ""+
//    			1000 * (finLocalDataMap.size() + finRoamDataMap.size()) 
//    			/(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2)));
//		
		//clear
        signalBeanList.clear();
        
	}
}


