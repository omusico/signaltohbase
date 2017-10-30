package com.asiainfo.HbaseDao;

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.util.EstimatableObjectWrapper;

import com.asiainfo.Bean.GetResultMaps;
import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.DataFormat;



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
            	//time               
            	System.out.println("10000 batch   "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
            	HbaseMain.time2=System.currentTimeMillis();          
            	
            	HbaseSignalPut(HbaseMain.dataList);
            	
      
            	HbaseMain.time2 = System.currentTimeMillis();      	
            	//time
            	System.out.println("10000 all     "+(10000*(System.currentTimeMillis()-HbaseMain.time)/(HbaseMain.batch)));
            	System.out.println("__________________________________________");
            	HbaseMain.time = System.currentTimeMillis();
            	HbaseMain.notFull.signalAll();//唤醒生产线程
            }
            HbaseMain.notEmpty.await();//阻塞消费线程
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
        	HbaseMain.lock.unlock();
        }
    }
    
	public synchronized void HbaseSignalPut(ArrayList<String[]> upValueList) {
		Set<UpDelTrans> dataMap = new HashSet<UpDelTrans>();
		Map<String,byte[]> resultLocalMap = new HashMap<String,byte[]>();
		Map<String,byte[]> resultRoamMap = new HashMap<String,byte[]>();
		
		
		Map<String, String> finLocalDataMap = new HashMap<String,String>();
		Map<String, String> finRoamDataMap = new HashMap<String,String>();

		DataFormat dataFormat = new DataFormat();
		HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();

		
		//hbase thread get 
		GetResultMaps getResultMaps = hbaseDaoThread.HbaseGetternew(HbaseDao.TABLE_NAME,upValueList);
		resultLocalMap = getResultMaps.getResultMap();
		resultRoamMap = getResultMaps.getRoamResultMap();
		
    	//time  
		System.out.println("10000 get     "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
    	List<UpDelTrans> updelTransList = new ArrayList<UpDelTrans>(); 

		//write batch
    	for(String[] upValue:upValueList){
    		UpDelTrans upDelTrans = new UpDelTrans();
			String Mdn = upValue[0];
			String value = upValue[1];
			String roam = upValue[2];
			
			byte[] hbase_local_value = resultLocalMap.get(Mdn);
			byte[] hbase_roam_value  = resultRoamMap.get(Mdn);
			
			if(hbase_local_value == null && hbase_roam_value==null){
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
			} else
			
			
			//from roam
			if(hbase_local_value == null && hbase_roam_value!=null){
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
			}
			updelTransList.add(upDelTrans);
			
//	    	for(UpDelTrans updelTransaa:updelTransList){
//	    		System.out.println("==================================");
//	    		List<String[]> DelArrayList = updelTransaa.getDelArrayList();
//	    		if(DelArrayList!=null){
//	        		for(int i=0;i<DelArrayList.size();i++){
//	            		for (String a:DelArrayList.get(i)){
//	            			System.out.print(a+",");
//	            		}
//	            		System.out.println("");
//	        		}
//	    		}
//	    	}
		}

		
    	//time
		System.out.println("10000 findata "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	

    	
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
    	
//    	for(Entry<String, String> entry : LocalDelMap.entrySet()){
//    		System.out.println("LocalDelMap++++++++++++++++");
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
//    	for(Entry<String, String> entry : LocalIndexDelMap.entrySet()){
//    		System.out.println("LocalIndexDelMap++++++++++++++++");
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
//    	for(Entry<String, String> entry : RoamIndexDelMap.entrySet()){
//    		System.out.println("RoamIndexDelMap++++++++++++++++");
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
    	
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME,LocalDelMap);
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME_INDEX,LocalIndexDelMap);
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_ROAM_NAME_INDEX,RoamIndexDelMap);
    	
    	

    	//hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME_INDEX,updelTransList);

		
    	//time  
		System.out.println("10000 delete  "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
//    	System.out.println("**************************************");
//    	for(Entry entry:finLocalDataMap.entrySet()){
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
//    	System.out.println("**************************************");
//    	for(Entry entry:finRoamDataMap.entrySet()){
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
    	
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseInput.STRING_rts,HbaseDao.TABLE_NAME_INDEX,finLocalDataMap);
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseInput.STRING_rtrs,HbaseDao.TABLE_ROAM_NAME_INDEX,finRoamDataMap);
    	//hbaseDaoThread.HbasePutter(HbaseDao.TABLE_ROAM_NAME,HbaseDao.TABLE_ROAM_NAME_INDEX,finRoamDataMap);
		
    	//time
		System.out.println("10000 put     "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
		
		//clear
		dataMap.clear();
		resultLocalMap.clear();
		finLocalDataMap.clear();
		finRoamDataMap.clear();
        HbaseMain.dataList.clear();
	}
	
	/**
	public synchronized void HbaseSignalPutold(ArrayList<String> Datalist) {
		Map<String,byte[]> dataMap = new LinkedHashMap<String, byte[]>();
		Map<String,byte[]> resultLocalMap = new HashMap<String,byte[]>();
		Map<String,byte[]> resultRoamMap = new HashMap<String,byte[]>();
		
		
		Map<String, String> finLocalDataMap = new HashMap<String,String>();
		Map<String, String> finRoamDataMap = new HashMap<String,String>();

		DataFormat dataFormat = new DataFormat();
		HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();
		int count = 0;		
		for (String Data : Datalist) {
			count++;
			String Mdn = Data.split(",", -1)[0];
			String Mdn_count =  Mdn+ "^" + count;
			String value = Data.substring(Data.indexOf(",")+1);
			
			dataMap.put(Mdn_count,Bytes.toBytes(value));
		}
		
		//hbase thread get 
		GetResultMaps getResultMaps = hbaseDaoThread.HbaseGetternew(HbaseDao.TABLE_NAME,dataMap.keySet());
		resultLocalMap = getResultMaps.getResultMap();
		resultRoamMap = getResultMaps.getRoamResultMap();
		
    	//time  
		System.out.println("10000 get     "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
    	List<UpDelTrans> updelTransList = new ArrayList<UpDelTrans>(); 

		//write batch
		for(String Mdn_count:dataMap.keySet()){
			String Mdn = Mdn_count.split("\\^")[0];
			String value = new String(dataMap.get(Mdn_count));
			String Data = Mdn+","+value;

			UpDelTrans updelTrans = new UpDelTrans();
			
			byte[] hbase_local_value = resultLocalMap.get(Mdn);
			byte[] hbase_roam_value  = resultRoamMap.get(Mdn);
			
			if(hbase_local_value == null && hbase_roam_value==null){
				updelTrans.setUpValue(Data);
				
				String UpValue = updelTrans.getUpValue();
				String[] finDatas = UpValue.split(",", -1);
				String finValue = UpValue.substring(UpValue.indexOf(",")+1,UpValue.lastIndexOf(","));
				
				if("local".equals(finDatas[finDatas.length-1])){
					resultLocalMap.put(Mdn, Bytes.toBytes(finValue));
					finLocalDataMap.put(Mdn, finValue);
					finRoamDataMap.remove(Mdn);
				}else if("roam".equals(finDatas[finDatas.length-1])){
					resultRoamMap.put(Mdn, Bytes.toBytes(finValue));
					finRoamDataMap.put(Mdn, finValue);
					finLocalDataMap.remove(Mdn);
				}
			}else
			
			//from local
			if(hbase_local_value != null && hbase_roam_value==null){
				String localresult = new String(resultLocalMap.get(Mdn));
				updelTrans = dataFormat.compareData(Data,localresult,HbaseDao.TABLE_NAME_INDEX);
				
				String UpValue = updelTrans.getUpValue();
				String[] finDatas = UpValue.split(",", -1);
				String finValue = UpValue.substring(UpValue.indexOf(",")+1,UpValue.lastIndexOf(","));
				
				if("local".equals(finDatas[finDatas.length-1])){
					resultLocalMap.put(Mdn, Bytes.toBytes(finValue));
					finLocalDataMap.put(Mdn, finValue);
				}else if("roam".equals(finDatas[finDatas.length-1])){
					resultRoamMap.put(Mdn, Bytes.toBytes(finValue));
					resultLocalMap.remove(Mdn);
					
					List<String[]> DelArrayList = new ArrayList<String[]>();
					if(updelTrans.getDelArrayList()!=null){
						DelArrayList = updelTrans.getDelArrayList();
					}
					DelArrayList.add(new String[]{HbaseDao.TABLE_NAME,Mdn,STRING_rts});
					updelTrans.setDelArrayList(DelArrayList);
		
					finRoamDataMap.put(Mdn, finValue);
					finLocalDataMap.remove(Mdn);
				}
			} else
			
			
			//from roam
			if(hbase_local_value == null && hbase_roam_value!=null){
				String roamresult = new String(resultRoamMap.get(Mdn));
				updelTrans = dataFormat.compareData(Data,roamresult,HbaseDao.TABLE_ROAM_NAME_INDEX);
				
				String UpValue = updelTrans.getUpValue();
				String[] finDatas = UpValue.split(",", -1);
				String finValue = UpValue.substring(UpValue.indexOf(",")+1,UpValue.lastIndexOf(","));

				if("local".equals(finDatas[finDatas.length-1])){
					resultLocalMap.put(Mdn, Bytes.toBytes(finValue));
					resultRoamMap.remove(Mdn);
					
					List<String[]> DelArrayList = new ArrayList<String[]>();
					if(updelTrans.getDelArrayList()!=null){
						DelArrayList = updelTrans.getDelArrayList();
					}
					DelArrayList.add(new String[]{HbaseDao.TABLE_ROAM_NAME,Mdn,STRING_rtrs});
					updelTrans.setDelArrayList(DelArrayList);
					finLocalDataMap.put(Mdn, finValue);
					finRoamDataMap.remove(Mdn);
				}else if("roam".equals(finDatas[finDatas.length-1])){
					resultRoamMap.put(Mdn, Bytes.toBytes(finValue));
					finRoamDataMap.put(Mdn, finValue);
				}
			}
			updelTransList.add(updelTrans);
			
//	    	for(UpDelTrans updelTransaa:updelTransList){
//	    		System.out.println("==================================");
//	    		List<String[]> DelArrayList = updelTransaa.getDelArrayList();
//	    		if(DelArrayList!=null){
//	        		for(int i=0;i<DelArrayList.size();i++){
//	            		for (String a:DelArrayList.get(i)){
//	            			System.out.print(a+",");
//	            		}
//	            		System.out.println("");
//	        		}
//	    		}
//	    	}
		}

		
    	//time
		System.out.println("10000 findata "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	

    	
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
    	
//    	for(Entry<String, String> entry : LocalDelMap.entrySet()){
//    		System.out.println("LocalDelMap++++++++++++++++");
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
//    	for(Entry<String, String> entry : LocalIndexDelMap.entrySet()){
//    		System.out.println("LocalIndexDelMap++++++++++++++++");
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
//    	for(Entry<String, String> entry : RoamIndexDelMap.entrySet()){
//    		System.out.println("RoamIndexDelMap++++++++++++++++");
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
    	
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME,LocalDelMap);
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME_INDEX,LocalIndexDelMap);
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_ROAM_NAME_INDEX,RoamIndexDelMap);
    	
    	

    	//hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME_INDEX,updelTransList);

		
    	//time  
		System.out.println("10000 delete  "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
//    	System.out.println("**************************************");
//    	for(Entry entry:finLocalDataMap.entrySet()){
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
//    	System.out.println("**************************************");
//    	for(Entry entry:finRoamDataMap.entrySet()){
//    		System.out.println(entry.getKey()+" "+entry.getValue());
//    	}
    	
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseInput.STRING_rts,HbaseDao.TABLE_NAME_INDEX,finLocalDataMap);
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseInput.STRING_rtrs,HbaseDao.TABLE_ROAM_NAME_INDEX,finRoamDataMap);
    	//hbaseDaoThread.HbasePutter(HbaseDao.TABLE_ROAM_NAME,HbaseDao.TABLE_ROAM_NAME_INDEX,finRoamDataMap);
		
    	//time
		System.out.println("10000 put     "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
		
		//clear
		dataMap.clear();
		resultLocalMap.clear();
		finLocalDataMap.clear();
		finRoamDataMap.clear();
        HbaseMain.dataList.clear();
	}
	*/
}


