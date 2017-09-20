package com.asiainfo.HbaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.asiainfo.Util.TimeFormat;


public class HbaseDaoThread{
	public static int taskSize = 1;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public HashMap<String, byte[]> HbaseGetter(String tableName,Set<String> rowKeys){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		Set<String> mdnSet = new HashSet<String>();
		ArrayList<Set<String>> allset = new ArrayList<Set<String>>();
		Set<HashMap<String, byte[]>> splitMapList = new HashSet<HashMap<String, byte[]>>();
		HashMap<String,byte[]> allMapList = new HashMap<String, byte[]>();
		
		for(int i=0;i<taskSize;i++){
			allset.add(new HashSet<String>());
		}
		
		for(String rowKey:rowKeys){
			mdnSet.add(rowKey.split("\\^")[0]);
		}
		
		int taskSizeNum = (mdnSet.size()/taskSize)+1;

		int mdncount=0;
		for(String mdn:mdnSet){
			allset.get(mdncount/taskSizeNum).add(mdn);
			mdncount++;
		}
		
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallableget(latch, tableName , allset.get(i)); 
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
				HTableInterface hTable = HbasePool.getHtable(tableName);
				for(String rowKey:rowKeys){
					GetList.add(new Get(Bytes.toBytes(rowKey.split("\\^")[0])));
				}
				results = hTable.get(GetList);
				hTable.close();
			
			for(Result result:results){
				if(result.getValue(HbaseInput.F_byte, HbaseInput.RTS_byte)!=null){
					resultMap.put(new String(result.getRow()),result.getValue(HbaseInput.F_byte, HbaseInput.RTS_byte));
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
				
				HTableInterface htInterface = HbasePool.getHtable(tableName);
                
				for(String line:lines){
					String[] lineparas = line.split("\\^",-1);
					String id = lineparas[0];
					String lacci = lineparas[1] + HbaseInput.lacci_split + lineparas[2];
					
					long time = Long.parseLong(lineparas[3])*60*1000;
					
					Get get = new Get(Bytes.toBytes(lacci));
					get.addFamily(HbaseInput.F_byte);
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
				HTableInterface htInterface = HbasePool.getHtable(tableName);
				for(String line:lines){
					String[] lineparas = line.split("\\^",-1);
					String id = lineparas[0];
					String lacci = lineparas[1] + HbaseInput.lacci_split + lineparas[2];
					long time = Long.parseLong(lineparas[3])*60*1000;
					
					Get get = new Get(Bytes.toBytes(lacci));
					get.addFamily(HbaseInput.F_byte);
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
	public void HbasePutter(String tableName,String tableindexName,Set<String[]> rowKeys){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		Set<String[]> mdnSet = new HashSet<String[]>();
		ArrayList<Set<String[]>> allset = new ArrayList<Set<String[]>>();
		
		for(int i=0;i<taskSize;i++){
			allset.add(new HashSet<String[]>());
		}
		
		for(String[] rowKey : rowKeys){
			mdnSet.add(rowKey);
		}
		
		int taskSizeNum = (mdnSet.size()/taskSize)+1;

		int mdncount=0;
		for(String[] mdn:mdnSet){
			allset.get(mdncount/taskSizeNum).add(mdn);
			mdncount++;
		}
		
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallableput(latch,tableName,tableindexName, allset.get(i)); 
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
		private Set<String[]> rowKeys;
		private String tableName;
		private String tableindexName;
		private CountDownLatch latch;
	
		public MyCallableput(CountDownLatch latch,String tableName,String tableindexName,Set<String[]> rowKeys) {
			this.latch = latch;
			this.rowKeys = rowKeys;
			this.tableName = tableName;
			this.tableindexName = tableindexName;
		}
		
		public Object call(){
		
			HbaseDao hbaseDao = new HbaseDao();
			HTableInterface[] htInterfaces = hbaseDao.MyInit(new String[]{tableName,tableindexName});
			for(String[] rowkey:rowKeys){
				long ts = new TimeFormat().Date2long(rowkey[3]);
				hbaseDao.putRow(htInterfaces[0], rowkey[0], HbaseInput.f, HbaseInput.rts, rowkey[2], ts);
				hbaseDao.putRow(htInterfaces[1], rowkey[1], HbaseInput.f, rowkey[0], rowkey[2].split(",",-1)[0], ts);	
			}
			hbaseDao.MyBufferFlush(htInterfaces);
			hbaseDao.MyClose(htInterfaces);
			latch.countDown();
			return null;
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void Hbasedelete(String tableindexName,ArrayList<Delete> DeleteSet){
		ExecutorService pool = Executors.newFixedThreadPool(taskSize); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		Set<Delete> oneSet = new HashSet<Delete>();
		ArrayList<ArrayList<Delete>> allSet = new ArrayList<ArrayList<Delete>>();
		
		for(int i=0;i<taskSize;i++){
			allSet.add(new ArrayList<Delete>());
		}
		
		for(Delete delete : DeleteSet){
			oneSet.add(delete);
		}
		
		int taskSizeNum = (oneSet.size()/taskSize)+1;

		int count=0;
		for(Delete one:oneSet){
			allSet.get(count/taskSizeNum).add(one);
			count++;
		}
		
		CountDownLatch latch =new CountDownLatch(taskSize);
		for(int i=0;i<taskSize;i++){
			Callable c = new MyCallabledelete(latch,tableindexName, allSet.get(i)); 
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
		private String tableindexName;
		private CountDownLatch latch;
		private HashMap<String, byte[]> resultMap = new HashMap<String, byte[]>();
	
		public MyCallabledelete(CountDownLatch latch,String tableindexName,ArrayList<Delete> rowKeys) {
			this.latch = latch;
			this.rowKeys = rowKeys;
			this.tableindexName = tableindexName;
		}
		
		public Object call(){
			HTableInterface htable = HbasePool.getHtable(tableindexName);
			try {
				htable.delete(rowKeys);
				htable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			latch.countDown();
			return resultMap;
		}
	}
	

}
