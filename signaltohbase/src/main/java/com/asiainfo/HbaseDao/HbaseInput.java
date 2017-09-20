package com.asiainfo.HbaseDao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.DataFormat;
import com.asiainfo.Util.TimeFormat;


public class HbaseInput{
	
	public final static String f = "f";
	public final static String rts = "rts";
	public final static String empty = "";
	public final static String lacci_split = "|";
	public final static byte[] F_byte = f.getBytes();
	public final static byte[] RTS_byte = rts.getBytes();
	public final static byte[] EMPTY_byte = "".getBytes();
	
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
	
	public synchronized void HbaseSignalPut(ArrayList<String> Datalist) {
		Map<String,byte[]> dataMap = new LinkedHashMap<String, byte[]>();
		Map<String,byte[]> resultMap = new HashMap<String,byte[]>();
		Set<String[]> finDataSet = new HashSet<String[]>();

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
		


		
		//hbase single or thread get 
//		resultMap = hbaseDao.getRowBatch(TABLE_NAME, dataMap.keySet());
		resultMap = hbaseDaoThread.HbaseGetter(HbaseDao.TABLE_NAME,dataMap.keySet());

    	//time  
		System.out.println("10000 get     "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
		//write batch
		for(String Mdn_count:dataMap.keySet()){
			String Mdn = Mdn_count.split("\\^")[0];
			String value = new String(dataMap.get(Mdn_count));
			String Data = Mdn+","+value;
			String finnalData = null;
			
			if (resultMap.get(Mdn) == null) {
				finnalData = Data;
			} else {
				String result = new String(resultMap.get(Mdn));
				finnalData = dataFormat.compareData(Data,result);
			}
			
			String[] finDatas = finnalData.split(",", -1);
			String finValue = finnalData.substring(finnalData.indexOf(",")+1);
			String lacci = finDatas[3]+lacci_split+finDatas[4];
			
//			HbaseMain.putSet.add(Mdn);
			finDataSet.add(new String[]{Mdn,lacci,finValue,finDatas[10]});
			resultMap.put(Mdn, Bytes.toBytes(finValue));
			
		}

		
    	//time
		System.out.println("10000 findata "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
    	hbaseDaoThread.Hbasedelete(HbaseDao.TABLE_NAME_INDEX,dataFormat.DeleteSet);
//		hbaseDao.delete(HbaseDao.TABLE_NAME_INDEX, dataFormat.DeleteSet);
		
    	//time  
		System.out.println("10000 delete  "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
    	
    	
    	hbaseDaoThread.HbasePutter(HbaseDao.TABLE_NAME,HbaseDao.TABLE_NAME_INDEX,finDataSet);
		
    	//time
		System.out.println("10000 put     "+(10000*(System.currentTimeMillis()-HbaseMain.time2)/(HbaseMain.batch)));
    	HbaseMain.time2=System.currentTimeMillis();
		
		//clear
		dataMap.clear();
		resultMap.clear();
		finDataSet.clear();
        HbaseMain.dataList.clear();
	}
}


