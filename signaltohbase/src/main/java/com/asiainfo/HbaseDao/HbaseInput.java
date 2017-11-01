package com.asiainfo.HbaseDao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.util.EstimatableObjectWrapper;

import com.asiainfo.Bean.GetResultMaps;
import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.DataFormat;
import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.TimeFormat;



public class HbaseInput{
	
	public final static String STRING_f = "f";
	public final static String STRING_rts = "rts";
	public final static String STRING_rtrs = "rtrs";
	public final static String STRING_empty = "";
	public final static String STRING_laccisplit = "|";
	public final static byte[] BYTE_f = STRING_f.getBytes();
	public final static byte[] BYTE_rts = STRING_rts.getBytes();
	public final static byte[] BYTE_rtrs = STRING_rtrs.getBytes();
	public final static byte[] BYTE_empty = "".getBytes();
	
    public void take() {  
        HbaseMain.lock.lock();
        try {  
            while (HbaseMain.dataList.size() >= HbaseMain.batch ){//如果队列满了
                      
            	HbaseMain.timeList.add(System.currentTimeMillis());
            	HbaseMain.timeMap.put("10000 batch: ", ""+
            			10000 *(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
            			/HbaseMain.dataList.size());
            	
            	try {
					HbaseSignalPut(HbaseMain.dataList);
				} catch (IOException e) {
					e.printStackTrace();
				}
            	
      
            	HbaseMain.timeList.add(System.currentTimeMillis());     
            	HbaseMain.timeMap.put("10000 all: ", ""+
            			10000 *(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(0))
            			/HbaseMain.batch);
            	
            	Log4JUtil.getTimeLog();
            	
            	
            	HbaseMain.notFull.signalAll();//唤醒生产线程
            }
            HbaseMain.notEmpty.await();//阻塞消费线程
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
        	HbaseMain.lock.unlock();
        }
    }
    
    
	public synchronized void HbaseSignalPut(ArrayList<String[]> upValueList) throws IOException {
		System.out.println("upValueList : " +upValueList.size());
		Set<UpDelTrans> dataMap = new HashSet<UpDelTrans>();
		Map<String,byte[]> resultLocalMap = new HashMap<String,byte[]>();
		Map<String,byte[]> resultRoamMap = new HashMap<String,byte[]>();
		
		
		Map<String, String> finLocalDataMap = new HashMap<String,String>();
		Map<String, String> finRoamDataMap = new HashMap<String,String>();

		DataFormat dataFormat = new DataFormat();
		HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();


		//hbase thread get 
		GetResultMaps getResultMaps = hbaseDaoThread.HbaseGetternew(HbaseDao.TABLE_NAME,upValueList);
    	//time  
		HbaseMain.timeList.add(System.currentTimeMillis());
    	HbaseMain.timeMap.put("10000 get: ", ""+
    			10000 *(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
    			/upValueList.size());
    	HbaseMain.timeMap.put("Get ops: ", ""+
    			1000 * upValueList.size() 
    			/(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2)));
    	
		resultLocalMap = getResultMaps.getResultMap();
		resultRoamMap = getResultMaps.getRoamResultMap();
		
		System.out.println(resultLocalMap.size()+" "+resultRoamMap.size());

    	
    	List<UpDelTrans> updelTransList = new ArrayList<UpDelTrans>(); 

		//write batch
    	int[] findtime = new int[]{0,0,0,0};
		
    	List<Delete> signalList = new ArrayList<Delete>();
    	List<Delete> signalindexList = new ArrayList<Delete>();
    	List<Delete> signalroamindexList = new ArrayList<Delete>();
    	for(String[] upValue:upValueList){
    		UpDelTrans upDelTrans = new UpDelTrans();
			String Mdn = upValue[0];
			String value = upValue[1];
			String roam = upValue[2];
			
			byte[] hbase_local_value = resultLocalMap.get(Mdn);
			byte[] hbase_roam_value  = resultRoamMap.get(Mdn);
			


				
			
			if(hbase_local_value == null && hbase_roam_value==null){
				findtime[0]++;
				String finValue = value;
				
				if("local".equals(roam)){
					resultLocalMap.put(Mdn, Bytes.toBytes(finValue));
					finLocalDataMap.put(Mdn, finValue);
					finRoamDataMap.remove(Mdn);
				}else if("roam".equals(roam)){
					resultRoamMap.put(Mdn, Bytes.toBytes(finValue));
					finRoamDataMap.put(Mdn, finValue);
					finLocalDataMap.remove(Mdn);
				}
			}else
			
				
			//from local
			if(hbase_local_value != null && hbase_roam_value==null){
				findtime[1]++;
				String localresult = new String(resultLocalMap.get(Mdn));
				upDelTrans = dataFormat.compareData(upValue,localresult,HbaseDao.TABLE_NAME_INDEX);
				upValue= upDelTrans.getUpValue();
				String finValue = upValue[1];
				
				if("local".equals(roam)){
					resultLocalMap.put(Mdn, Bytes.toBytes(finValue));
					finLocalDataMap.put(Mdn, finValue);
				}else if("roam".equals(roam)){
					resultRoamMap.put(Mdn, Bytes.toBytes(finValue));
					resultLocalMap.remove(Mdn);
					
					List<String[]> DelArrayList = new ArrayList<String[]>();
					if(upDelTrans.getDelArrayList()!=null){
						DelArrayList = upDelTrans.getDelArrayList();
					}
					DelArrayList.add(new String[]{HbaseDao.TABLE_NAME,Mdn,STRING_rts});
					upDelTrans.setDelArrayList(DelArrayList);
		
					finRoamDataMap.put(Mdn, finValue);
					finLocalDataMap.remove(Mdn);
				}
			}else
			
			
			//from roam
			if(hbase_local_value == null && hbase_roam_value!=null){
				findtime[2]++;
				String roamresult = new String(resultRoamMap.get(Mdn));
				upDelTrans = dataFormat.compareData(upValue,roamresult,HbaseDao.TABLE_ROAM_NAME_INDEX);
				upValue= upDelTrans.getUpValue();
				String finValue = upValue[1];

				if("local".equals(roam)){
					resultLocalMap.put(Mdn, Bytes.toBytes(finValue));
					resultRoamMap.remove(Mdn);
					
					List<String[]> DelArrayList = new ArrayList<String[]>();
					if(upDelTrans.getDelArrayList()!=null){
						DelArrayList = upDelTrans.getDelArrayList();
					}
					DelArrayList.add(new String[]{HbaseDao.TABLE_ROAM_NAME,Mdn,STRING_rtrs});
					upDelTrans.setDelArrayList(DelArrayList);
					finLocalDataMap.put(Mdn, finValue);
					finRoamDataMap.remove(Mdn);
				}else if("roam".equals(roam)){
					resultRoamMap.put(Mdn, Bytes.toBytes(finValue));
					finRoamDataMap.put(Mdn, finValue);
				}
			}else if(hbase_local_value != null && hbase_roam_value!=null){
				findtime[3]++;
				
				String localresult = new String(resultLocalMap.get(Mdn));
				String roamresult = new String(resultRoamMap.get(Mdn));
				String[] localresults = localresult.split(",", -1);	
				String[] roamresults = roamresult.split(",", -1);	
				
				if (new TimeFormat().Date2long(localresults[9]) >= new TimeFormat().Date2long(roamresults[9])) {
					
					Delete delete = new Delete(Bytes.toBytes(Mdn));
					delete.addColumn(HbaseInput.BYTE_f, HbaseInput.BYTE_rtrs);
					Delete deleteroamindex = new Delete(Bytes.toBytes(roamresults[2]+HbaseInput.STRING_laccisplit+roamresults[3]));
					deleteroamindex.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(Mdn));

					signalList.add(delete);
					signalroamindexList.add(deleteroamindex);	
				}else{

					Delete delete = new Delete(Bytes.toBytes(Mdn));
					delete.addColumn(HbaseInput.BYTE_f, HbaseInput.BYTE_rts);
					Delete deleteindex = new Delete(Bytes.toBytes(localresults[2]+HbaseInput.STRING_laccisplit+localresults[3]));
					deleteindex.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(Mdn));
					
					signalList.add(delete);
					signalindexList.add(deleteindex);				

				}
				
			}
			updelTransList.add(upDelTrans);

		}
		Table htable = HbasePool.getHtable(HbaseDao.TABLE_NAME);
		Table htableindex = HbasePool.getHtable(HbaseDao.TABLE_NAME_INDEX);
		Table htableroamindex = HbasePool.getHtable(HbaseDao.TABLE_ROAM_NAME_INDEX);
		htable.delete(signalList);
		htable.close();
		htableindex.delete(signalindexList);
		htableindex.close();
		htableroamindex.delete(signalroamindexList);
		htableindex.close();
    	
    	
		HbaseMain.timeList.add(System.currentTimeMillis());
    	HbaseMain.timeMap.put("10000 finvalue: ", ""+
    			10000 *(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
    			/upValueList.size() );
    	HbaseMain.timeMap.put("finvalue ops: ", ""+
    			1000 * upValueList.size() 
    			/(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
    			+" ["+findtime[0]+" "
    				+findtime[1]+" "
    				+findtime[2]+" "
    				+findtime[3]+" "
    			+"]");
    	
    	
    	Map<String,String> LocalDelMap = new HashMap<String,String>();
    	
    	Map<String,String> LocalIndexDelMap = new HashMap<String,String>();
    	Map<String,String> RoamIndexDelMap = new HashMap<String,String>();
    	
    	
    	for(UpDelTrans updelTrans:updelTransList){
    		List<String[]> DelArrayList = updelTrans.getDelArrayList();
    		if(DelArrayList!=null){
    			for(String[] DelList:DelArrayList){
    				if(DelList[0].endsWith("signal")){
    					if((!LocalDelMap.containsKey(DelList[1]))){
    						LocalDelMap.put(DelList[1],DelList[2]);
    					}
    				}else if(DelList[0].endsWith("index")){
    					if((!LocalIndexDelMap.containsKey(DelList[1])) && (!RoamIndexDelMap.containsKey(DelList[1]))){
    						if(HbaseDao.TABLE_NAME_INDEX.equals(DelList[0])){
    							LocalIndexDelMap.put(DelList[1],DelList[2]);
    						}else if(HbaseDao.TABLE_ROAM_NAME_INDEX.equals(DelList[0])){
    							RoamIndexDelMap.put(DelList[1],DelList[2]);
    						}
    					}
    				}    			
    			}
    		}
    	}
    	
    	HbaseMain.timeList.add(System.currentTimeMillis());
    

    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME,LocalDelMap);
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME_INDEX,LocalIndexDelMap);
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_ROAM_NAME_INDEX,RoamIndexDelMap);
    	
    	
		HbaseMain.timeList.add(System.currentTimeMillis());
    	HbaseMain.timeMap.put("10000 delete: ",""+ 
    			10000 *(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
    			/upValueList.size() );
    	HbaseMain.timeMap.put("delete ops: ", ""+
    			1000 * (LocalDelMap.size() + LocalIndexDelMap.size() + RoamIndexDelMap.size()) 
    			/(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
    			+" ["+
    				LocalDelMap.size() +", "+ LocalIndexDelMap.size() +", "+ RoamIndexDelMap.size()
    			+"]");
    	

    	
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseInput.STRING_rts,HbaseDao.TABLE_NAME_INDEX,finLocalDataMap);
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseInput.STRING_rtrs,HbaseDao.TABLE_ROAM_NAME_INDEX,finRoamDataMap);

		
    	HbaseMain.timeList.add(System.currentTimeMillis());
    	HbaseMain.timeMap.put("10000 put: ", ""+
    			10000 *(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2))
    			/upValueList.size() );
    	HbaseMain.timeMap.put("put ops: ", ""+
    			1000 * (finLocalDataMap.size() + finRoamDataMap.size()) 
    			/(HbaseMain.timeList.get(HbaseMain.timeList.size()-1)- HbaseMain.timeList.get(HbaseMain.timeList.size()-2)));
		
		//clear
		dataMap.clear();
		resultLocalMap.clear();
		finLocalDataMap.clear();
		finRoamDataMap.clear();
        HbaseMain.dataList.clear();
	}
}


