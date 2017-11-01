package com.asiainfo.HbaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.*;

import com.asiainfo.Bean.GetResultMaps;
import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.TimeFormat;


public class HbaseDaoThread{
	public static int taskSize = 1;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public HashMap<String, byte[]> HbaseGetter(String tableName,Set<String> rowKeys){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<String> mdnArrayList = new ArrayList<String>();
		ArrayList<HashSet<String>> allArrayList = new ArrayList<HashSet<String>>();
		Set<HashMap<String, byte[]>> splitMapList = new HashSet<HashMap<String, byte[]>>();
		HashMap<String,byte[]> allMapList = new HashMap<String, byte[]>();

		for(int i=0;i<taskSize;i++){
			allArrayList.add(new HashSet<String>());
		}

		for(String rowKey:rowKeys){
			mdnArrayList.add(rowKey.split("\\^")[0]);
		}
		
		Collections.sort(mdnArrayList);

		int taskSizeNum = (mdnArrayList.size()/taskSize)+1;
		


		int mdncount=0;
		for(String mdn:mdnArrayList){
			allArrayList.get(mdncount/taskSizeNum).add(mdn);
			mdncount++;
		}

		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallableget(latch, tableName , allArrayList.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		pool.shutdown();
		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		for(Future f:list){
			try {
				splitMapList.add((HashMap<String, byte[]>)f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		for(HashMap<String,byte[]> oneMap:splitMapList){
			allMapList.putAll(oneMap);
		}
		return allMapList;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public GetResultMaps HbaseGetternew(String tableName,List<String[]> upValueList){
		
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<String> mdnArrayList = new ArrayList<String>();
		ArrayList<List<String>> allArrayList = new ArrayList<List<String>>();
		Set<GetResultMaps> splitMapList = new HashSet<GetResultMaps>();
		GetResultMaps getAllResultMaps = new GetResultMaps();
		HashMap<String,byte[]> allMapList = new HashMap<String, byte[]>();
		HashMap<String,byte[]> allRoamMapList = new HashMap<String, byte[]>();

		for(int i=0;i<taskSize;i++){
			allArrayList.add(new ArrayList<String>());
		}

		for(String[] upValue:upValueList){
			mdnArrayList.add(upValue[0]);
		}

		int taskSizeNum = (mdnArrayList.size()/taskSize)+1;

		int mdncount=0;
		for(String mdn:mdnArrayList){
			allArrayList.get(mdncount/taskSizeNum).add(mdn);
			mdncount++;
		}
		CountDownLatch latch =new CountDownLatch(taskSize);

		
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallableget_Version1_2(latch, tableName , allArrayList.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		pool.shutdown();
		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		for(Future f:list){
			try {
				splitMapList.add((GetResultMaps)f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		for(GetResultMaps getResultMaps:splitMapList){
			allMapList.putAll(getResultMaps.getResultMap());
			allRoamMapList.putAll(getResultMaps.getRoamResultMap());
		}
		getAllResultMaps.setResultMap(allMapList);
		getAllResultMaps.setRoamResultMap(allRoamMapList);
		
		return getAllResultMaps;
	}
	
	private class MyCallableget_Version1_2 implements Callable<Object> {
		private List<String> rowKeys;
		private String tableName;
		private Result[] results;
		private CountDownLatch latch;
		private ArrayList<Get> GetList = new ArrayList<Get>();
		private HashMap<String, byte[]> resultMap = new HashMap<String, byte[]>();
		private HashMap<String, byte[]> roamResultMap = new HashMap<String, byte[]>();
	
		public MyCallableget_Version1_2(CountDownLatch latch,String tableName ,List<String> rowKeys) {
			this.latch = latch;
			this.rowKeys = rowKeys;
			this.tableName = tableName;
		}
		
		public Object call(){
			try {
				Table hTable = HbasePool.getHtable(tableName);

				for(String rowKey:rowKeys){
					Get get = new Get(Bytes.toBytes(rowKey));
					get.addFamily(HbaseInput.BYTE_f);
					GetList.add(get);
				}
				//System.out.println(Thread.currentThread().getName()+" 111 : "+10000*(timeb-timea)/HbaseMain.batch);
//				boolean[] booleans = hTable.existsAll(GetList);
//				long timec = System.currentTimeMillis();
//				System.out.println(Thread.currentThread().getName()+" 222 : "+10000*(timec-timeb)/HbaseMain.batch);
//				for(int i=0;i<GetList.size();i++){
//					if(booleans[i]){
//						GetListExist.add(GetList.get(i));
//					}else{
//						resultMap.put(new String(GetList.get(i).getRow()),null);
//						roamResultMap.put(new String(GetList.get(i).getRow()),null);
//					}
//				}
				
				
				results = hTable.get(GetList);

				hTable.close();
				
				for(Result result:results){
					if(result.getRow()!=null){
						resultMap.put(new String(result.getRow()),result.getValue(HbaseInput.BYTE_f, HbaseInput.BYTE_rts));
						roamResultMap.put(new String(result.getRow()),result.getValue(HbaseInput.BYTE_f, HbaseInput.BYTE_rtrs));
					}
				}	

			} catch (IOException e) {
				e.printStackTrace();
			}
			latch.countDown();
			GetResultMaps getResultMaps = new GetResultMaps();
			getResultMaps.setResultMap(resultMap);
			getResultMaps.setRoamResultMap(roamResultMap);
			
			return getResultMaps;
		}
	}
	
	private class MyCallableget implements Callable<Object> {
		private Set<String> rowKeys;
		private String tableName;
		private Result[] results;
		private CountDownLatch latch;
		private ArrayList<Get> GetList = new ArrayList<Get>();
		private HashMap<String, byte[]> resultMap = new HashMap<String, byte[]>();
	
		public MyCallableget(CountDownLatch latch,String tableName ,Set<String> rowKeys) {
			this.latch = latch;
			this.rowKeys = rowKeys;
			this.tableName = tableName;
		}
		
		public Object call(){
			try {
				HbaseDao hbaseDao = new HbaseDao();
				Map<String, HTableInterface> htInterfaces = hbaseDao.MyInit(new String[]{tableName});
				HTableInterface hTable = htInterfaces.get(tableName);
				for(String rowKey:rowKeys){
					Get getTmp = new Get(Bytes.toBytes(rowKey.split("\\^")[0]));
					GetList.add(getTmp);
				}
				results = hTable.get(GetList);
				hTable.close();
			
			for(Result result:results){
				if(result.getValue(HbaseInput.BYTE_f, HbaseInput.BYTE_rts)!=null){
					resultMap.put(new String(result.getRow()),result.getValue(HbaseInput.BYTE_f, HbaseInput.BYTE_rts));
				}
			}			
			} catch (IOException e) {
				e.printStackTrace();
			}
			latch.countDown();
			return resultMap;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Set<String> HbaseIndexGetter(String tableName,Set<String> lines){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		ArrayList<Set<String>> allset = new ArrayList<Set<String>>();
		Set<String> allSetList = new HashSet<String>();
		
		for(int i=0;i<taskSize;i++){
			allset.add(new HashSet<String>());
		}
		
		int taskSizeNum = (lines.size()/taskSize)+1;

		int linecount=0;
		for(String line:lines){
			allset.get(linecount/taskSizeNum).add(line);
			linecount++;
		}
		
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyIndexCallableget(latch , tableName ,allset.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		
		pool.shutdown();
		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		for(Future f:list){
			try {
				allSetList.addAll((HashSet<String>)f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		return allSetList;
	}
	
	private class MyIndexCallableget implements Callable<Object> {
		private Set<String> lines;
		private String tableName;
		private CountDownLatch latch;
		private Set<String> results = new HashSet<String>();
	
		public MyIndexCallableget(CountDownLatch latch,String tableName ,Set<String> lines) {
			this.latch = latch;
			this.lines = lines;
			this.tableName = tableName;
		}
		
		public Object call(){
			try {
				
				HTableInterface htInterface = HbasePoolOld.getHtable(tableName);
                
				for(String line:lines){
					String[] lineparas = line.split("\\^",-1);
					String id = lineparas[0];
					String lacci = lineparas[1] + HbaseInput.STRING_laccisplit + lineparas[2];
					
					long time = Long.parseLong(lineparas[3])*60*1000;
					
					Get get = new Get(Bytes.toBytes(lacci));
					get.addFamily(HbaseInput.BYTE_f);
					long current = System.currentTimeMillis();
					get.setTimeRange(current-time, current);
					
					Result result = htInterface.get(get);
					CellScanner cs = result.cellScanner();
					
					while(cs.advance()){
						Cell cell = cs.current();
						String qualifier = new String(CellUtil.cloneQualifier(cell));
						byte[] valuebyte = CellUtil.cloneValue(cell);
						if(valuebyte!=null){
							String value = new String(valuebyte);
							if(!value.equals("") && Integer.parseInt(value) >=570 && Integer.parseInt(value) <=580){
								results.add(id+"^"+qualifier);
							}
						}
//						results.add(id+"^"+qualifier);
					}
				}
				htInterface.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			latch.countDown();
			return results;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Map<String, Set<String>> HbaseIndexGetter1(String tableName,Set<String> lines){
		System.out.println("taskSize"+taskSize);
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		ArrayList<Set<String>> allset = new ArrayList<Set<String>>();
		Map<String, Set<String>> allSetList = new HashMap<String, Set<String>>();
		
		for(int i=0;i<taskSize;i++){
			allset.add(new HashSet<String>());
		}
		
		int taskSizeNum = (lines.size()/taskSize)+1;

		int linecount=0;
		for(String line:lines){
			allset.get(linecount/taskSizeNum).add(line);
			linecount++;
		}
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyIndexCallableget1(latch,tableName ,allset.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		
		pool.shutdown();
		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		for(Future f:list){
			try {
				allSetList.putAll((Map<String, Set<String>>)f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		return allSetList;
	}
	
	private class MyIndexCallableget1 implements Callable<Object> {
		private Set<String> lines;
		private String tableName;
		private CountDownLatch latch;
		private Map<String, Set<String>> re = new HashMap<String,Set<String>>();
	
		public MyIndexCallableget1(CountDownLatch latch,String tableName ,Set<String> lines) {
			this.latch = latch;
			this.lines = lines;
			this.tableName = tableName;
		}
		
		public Object call(){
			try {
				HTableInterface htInterface = HbasePoolOld.getHtable(tableName);
				for(String line:lines){
					String[] lineparas = line.split("\\^",-1);
					String id = lineparas[0];
					String lacci = lineparas[1] + HbaseInput.STRING_laccisplit + lineparas[2];
					long time = Long.parseLong(lineparas[3])*60*1000;
					
					Get get = new Get(Bytes.toBytes(lacci));
					get.addFamily(HbaseInput.BYTE_f);
					long current = System.currentTimeMillis();
					get.setTimeRange(current-time, current);
					
					Result result = htInterface.get(get);
					CellScanner cs = result.cellScanner();
					
					Set<String> mdnset = new HashSet<String>();
					while(cs.advance()){
						Cell cell = cs.current();
						String qualifier = new String(CellUtil.cloneQualifier(cell));
						mdnset.add(qualifier);
					}
					re.put(id+"^"+lacci, mdnset);
				}
				htInterface.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			latch.countDown();
			return re;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void HbasePutter(String tableName,String qualifier,String tableindexName,Map<String,String> rowKeys){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		Set<Entry<String, String>> mdnSet = new HashSet<Entry<String, String>>();
		ArrayList<Set<Entry<String, String>>> allset = new ArrayList<Set<Entry<String, String>>>();
		
		for(int i=0;i<taskSize;i++){
			allset.add(new HashSet<Entry<String, String>>());
		}
		
		for(Entry rowKey : rowKeys.entrySet()){
			mdnSet.add(rowKey);
		}
		
		int taskSizeNum = (mdnSet.size()/taskSize)+1;

		int mdncount=0;
		for(Entry<String, String> mdn:mdnSet){
			allset.get(mdncount/taskSizeNum).add(mdn);
			mdncount++;
		}
		
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallableput(latch,tableName,qualifier,tableindexName, allset.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}

		pool.shutdown();
		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
	
	private class MyCallableput implements Callable<Object> {
		private Set<Entry<String, String>> rowKeys;
		private String tableName;
		private String qualifier;
		private String tableindexName;
		private CountDownLatch latch;
	
		public MyCallableput(CountDownLatch latch,String tableName,String qualifier,String tableindexName,Set<Entry<String, String>> rowKeys) {
			this.latch = latch;
			this.rowKeys = rowKeys;
			this.tableName = tableName;
			this.qualifier = qualifier;
			this.tableindexName = tableindexName;
		}
		
		public Object call(){
		
			HbaseDao hbaseDao = new HbaseDao();
			Map<String, HTableInterface> htInterfaces = hbaseDao.MyInit(new String[]{tableName,tableindexName});

			for(Entry<String,String> rowkey:rowKeys){
				String Mdn = rowkey.getKey();
				String Value = rowkey.getValue();
				String[] finDatas = Value.split(",", -1);
				String lacci = finDatas[2]+HbaseInput.STRING_laccisplit+finDatas[3];
				long ts = new TimeFormat().Date2long(finDatas[9]);
				hbaseDao.putRow(htInterfaces.get(tableName), Mdn, HbaseInput.STRING_f, qualifier, Value, ts);
				hbaseDao.putRow(htInterfaces.get(tableindexName), lacci, HbaseInput.STRING_f, Mdn, Value.split(",",-1)[0], ts);	
			}
			hbaseDao.MyBufferFlush(new ArrayList<HTableInterface>(htInterfaces.values()));
			hbaseDao.MyClose(new ArrayList<HTableInterface>(htInterfaces.values()));
			latch.countDown();
			return null;
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void Hbasedelete(String tableName,List<UpDelTrans> updelTransList){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		Set<Delete> oneSet = new HashSet<Delete>();
		ArrayList<ArrayList<Delete>> allSet = new ArrayList<ArrayList<Delete>>();
		
		for(int i=0;i<taskSize;i++){
			allSet.add(new ArrayList<Delete>());
		}
		
		for(UpDelTrans updelTrans:updelTransList){
			String[] DelStringArray = null;
			if(tableName.endsWith("signalindex")){
//				DelStringArray = updelTrans.getDelTableIndex();
			}else if(tableName.endsWith("signal")){
//				DelStringArray = updelTrans.getDelTableAnother();
			}
			if(DelStringArray!=null){
				Delete delete = new Delete(Bytes.toBytes(DelStringArray[0]));
				delete.deleteColumns(HbaseInput.BYTE_f, Bytes.toBytes(DelStringArray[1]));
				delete.setDurability(Durability.SKIP_WAL);
				oneSet.add(delete);
			}
		}
		
		int taskSizeNum = (oneSet.size()/taskSize)+1;

		int count=0;
		for(Delete one:oneSet){
			allSet.get(count/taskSizeNum).add(one);
			count++;
		}
		
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallabledelete(latch,tableName, allSet.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void Hbasedelete(String tableName,Map<String,String> DeleteSet){
		int tasksize = taskSize;
		if(DeleteSet.size()<taskSize){
			tasksize = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(tasksize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		Set<Delete> oneSet = new HashSet<Delete>();
		ArrayList<ArrayList<Delete>> allSet = new ArrayList<ArrayList<Delete>>();
		
		for(int i=0;i<tasksize;i++){
			allSet.add(new ArrayList<Delete>());
		}
		
		for(Entry<String,String> deleteEntry : DeleteSet.entrySet()){
			Delete delete = null;
			if(tableName.endsWith("signal")){
				delete = new Delete(Bytes.toBytes(deleteEntry.getKey()));
				delete.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(deleteEntry.getValue()));
			}else if(tableName.endsWith("index")){
				delete = new Delete(Bytes.toBytes(deleteEntry.getValue()));
				delete.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(deleteEntry.getKey()));
			}
			delete.setDurability(Durability.SKIP_WAL);
			oneSet.add(delete);
		}
		int taskSizeNum = (oneSet.size()/tasksize)+1;
		


		int count=0;
		for(Delete one:oneSet){
			allSet.get(count/taskSizeNum).add(one);
			count++;
		}
		
		CountDownLatch latch =new CountDownLatch(tasksize);
		for(int i=0;i<tasksize;i++){
			Callable c = new MyCallabledelete(latch,tableName, allSet.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();
	}
	
	private class MyCallabledelete implements Callable<Object> {
		private ArrayList<Delete> rowKeys;
		private String tableName;
		private CountDownLatch latch;
		private HashMap<String, byte[]> resultMap = new HashMap<String, byte[]>();
	
		public MyCallabledelete(CountDownLatch latch,String tableindexName,ArrayList<Delete> rowKeys) {
			this.latch = latch;
			this.rowKeys = rowKeys;
			this.tableName = tableindexName;
		}
		
		public Object call(){
			Map<String, HTableInterface> htInterfaces = new HbaseDao().MyInit(new String[]{tableName});
			HTableInterface htable = htInterfaces.get(tableName);
				try {
				htable.delete(rowKeys);
				htable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
//			Table htable = HbasePool.getHtable(tableName);
//			try {
//				htable.delete(rowKeys);
//				htable.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			latch.countDown();
			return resultMap;
		}
	}
	

}
