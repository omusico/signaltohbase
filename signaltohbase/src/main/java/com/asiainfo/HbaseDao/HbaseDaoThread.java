package com.asiainfo.HbaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.*;

import com.asiainfo.Bean.GetResultMaps;
import com.asiainfo.Bean.IndexBean;
import com.asiainfo.Bean.SignalBean;
import com.asiainfo.Bean.UpDelTrans;
import com.asiainfo.Bean.UpValueForMutate;
import com.asiainfo.Main.HbaseMainOld;
import com.asiainfo.Util.DataFormat;
import com.asiainfo.Util.ExtractData;
import com.asiainfo.Util.ParamUtil;
import com.asiainfo.Util.TimeFormat;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion.Static;

import scala.annotation.meta.param;


public class HbaseDaoThread{
	ParamUtil paramUtil = ParamUtil.getInstance();
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Set<SignalBean> HbaseGetternew(String tableName,Set<SignalBean> signalBeanSet){
		int Get_POOL_SIZE = paramUtil.GET_POOL_SIZE;
		ExecutorService pool = Executors.newFixedThreadPool(paramUtil.GET_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<List<SignalBean>> splitSignalBeanSetList = new ArrayList<List<SignalBean>>();
		Set<SignalBean> SignalBeanSet = new HashSet<SignalBean>();
		
		for(int i=0; i < Get_POOL_SIZE; i++){
			splitSignalBeanSetList.add(new ArrayList<SignalBean>());
		}

		int taskSizeNum = (signalBeanSet.size()/Get_POOL_SIZE)+1;

		int mdncount=0;
		for(SignalBean signalBean:signalBeanSet){
			splitSignalBeanSetList.get(mdncount/taskSizeNum).add(signalBean);
			mdncount++;
		}
		CountDownLatch latch = new CountDownLatch(Get_POOL_SIZE);

		
		for(int i=0;i<Get_POOL_SIZE;i++){
			Callable c = new MyCallableget_Version1_2(latch, tableName , splitSignalBeanSetList.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
		pool.shutdown();
//		try {
//			latch.await();
//		} catch (InterruptedException e1) {
//			e1.printStackTrace();
//		}

		for(Future f:list){
			try {
				SignalBeanSet.addAll((HashSet<SignalBean>)f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		return SignalBeanSet;
	}
	
	private class MyCallableget_Version1_2 implements Callable<Object> {
		private List<SignalBean> signalBeanList;
		private String tableName;
		private Result[] results;
		private CountDownLatch latch;
		private ArrayList<Get> GetList = new ArrayList<Get>();
		private HashSet<SignalBean> resultSet = new HashSet<SignalBean>();
	
		public MyCallableget_Version1_2(CountDownLatch latch,String tableName ,List<SignalBean> signalBeanList) {
			this.latch = latch;
			this.signalBeanList = signalBeanList;
			this.tableName = tableName;
		}
		
		public Object call(){
			try {
				Table hTable = HbasePool.getHtable(tableName);
				Map<String, SignalBean> signalMap = new HashMap<String, SignalBean>();
				
				for(SignalBean signalBean:signalBeanList){
					signalBean.setIsGet(true);
					String mdn = signalBean.getKafkaValues().get(0);
					signalMap.put(mdn, signalBean);
					Get get = new Get(Bytes.toBytes(mdn));
					get.addFamily(HbaseInput.BYTE_f);
					GetList.add(get);
				}
				results = hTable.get(GetList);
				hTable.close();
				
				for(Result result:results){
					if(result.getRow()!=null){
						String mdn = new String(result.getRow());
						
						SignalBean signalBean = signalMap.get(mdn);
						String value = new String(result.getValue(HbaseInput.BYTE_f, HbaseInput.BYTE_rts));
						String[] values = (mdn+","+value).split(",",-1);
						
//						String[] values = new String[]{
//								mdn,
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_signalType   )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_area         )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_roam         )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_roamProv     )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_roamCountry  )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_lac          )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_ci           )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_longitude    )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_latitude     )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_imsi         )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_imei         )),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_currentCiTime)),
//								ExtractData.bytesToString(result.getValue(HbaseInput.BYTE_f, HbaseInput.bytes_lastTime     )) 								
//						};
//						
						signalBean.setHbaseValues(Arrays.asList(values));
						
						signalMap.put(mdn, signalBean);
					}
				}	
				for(Entry<String, SignalBean> entry:signalMap.entrySet()){
					resultSet.add(entry.getValue());
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
//			latch.countDown();
			
			return resultSet;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Set<SignalBean> HbaseUpValue(Set<SignalBean> signalBeanSet){
		ParamUtil params = ParamUtil.getInstance();
		
		if(signalBeanSet.size() < params.UPVALUE_POOL_SIZE){
			params.UPVALUE_POOL_SIZE = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(params.UPVALUE_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 
		
		ArrayList<Set<SignalBean>> SplitMdnSetList = new ArrayList<Set<SignalBean>>();
		
		for(int i=0; i < params.UPVALUE_POOL_SIZE; i++){
			SplitMdnSetList.add(new HashSet<SignalBean>());
		}
		
		int SplitMdnSetSize = (signalBeanSet.size()/params.UPVALUE_POOL_SIZE)+1;

		int size = 0;
		for(SignalBean signalBean :  signalBeanSet){
			SplitMdnSetList.get(size/SplitMdnSetSize).add(signalBean);
			size++;
		}


		CountDownLatch latch =new CountDownLatch(params.UPVALUE_POOL_SIZE);
		for(int i=0;i<params.UPVALUE_POOL_SIZE;i++){
			Callable c = new MyCallableUpValue(latch , SplitMdnSetList.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
//		try {
//			latch.await();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		
		Set<SignalBean> allResultSignalBeanSet = new HashSet<SignalBean>();
		for(Future f:list){
			try {
				Set<SignalBean> resultSignalBeanSet = (Set<SignalBean>)f.get();
				
				allResultSignalBeanSet.addAll(resultSignalBeanSet);
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		pool.shutdown();
		
		return allResultSignalBeanSet;
	}
	
	private class MyCallableUpValue implements Callable<Object> {
		private CountDownLatch latch;
		private Set<SignalBean> signalBeanSet;
	
		public MyCallableUpValue(CountDownLatch latch, Set<SignalBean> signalBeanSet) {
			this.latch = latch;
			this.signalBeanSet = signalBeanSet;
		}
		
		public Object call(){
			DataFormat dataFormat = new DataFormat();
			Set<SignalBean> resultSignalBeanSet = new HashSet<SignalBean>();
	    	
			for(SignalBean signalBean : signalBeanSet){
				signalBean = dataFormat.compareSignalBean(signalBean);
				signalBean.setHbaseValues(null);
				resultSignalBeanSet.add(signalBean);
			}
			
			return resultSignalBeanSet;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void HbaseMutatorDel(String table, Set<SignalBean> resultSignalBeanSet){
		ParamUtil params = ParamUtil.getInstance();
		
		if(resultSignalBeanSet.size() < params.MUTATEDEL_POOL_SIZE){
			params.MUTATEDEL_POOL_SIZE = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(params.MUTATEDEL_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<List<SignalBean>> SplitDelIndexBeanListList = new ArrayList<List<SignalBean>>();
		
		for(int i=0; i < params.MUTATEDEL_POOL_SIZE; i++){
			SplitDelIndexBeanListList.add(new ArrayList<SignalBean>());
		}
		
		int SplitDelIndexBeanListSize = (resultSignalBeanSet.size()/params.MUTATEDEL_POOL_SIZE)+1;
		int num = 0;
		for(SignalBean signalBean : resultSignalBeanSet){
			SplitDelIndexBeanListList.get(num/SplitDelIndexBeanListSize).add(signalBean);
		}

		CountDownLatch latch =new CountDownLatch(params.MUTATEDEL_POOL_SIZE);
		for(int i=0;i<params.MUTATEDEL_POOL_SIZE;i++){
			Callable c = new MyCallableMutateDel(latch, table, SplitDelIndexBeanListList.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}
//		try {
//			latch.await();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		pool.shutdown();
	}
	
	private class MyCallableMutateDel implements Callable<Object> {
		private CountDownLatch latch;
		private String table;
		private List<SignalBean> delIndexBeanList;
		
	
		public MyCallableMutateDel(CountDownLatch latch, String table, List<SignalBean> delIndexBeanList) {
			this.latch = latch;
			this.table = table;
			this.delIndexBeanList = delIndexBeanList;
		}
		
		public Object call(){
			BufferedMutator mutator = HbasePool.getMutator(table, ParamUtil.getInstance().MUTATEDEL_WRITEBUFFERSIZE);
			try {
				for(SignalBean signalBean : delIndexBeanList){
					IndexBean indexBean = signalBean.getDelIndexBeanList().get(0);
					Delete delete = new Delete(Bytes.toBytes(indexBean.getRowkey()));
					delete.addColumn(Bytes.toBytes(indexBean.getCf()),Bytes.toBytes(indexBean.getCq()));
					delete.setDurability(Durability.SKIP_WAL);
					mutator.mutate(delete);
				}
				mutator.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
//			latch.countDown();
			return null;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void HbasePutter(Set<SignalBean> signalBeanList){
		ParamUtil params = ParamUtil.getInstance();
		int MutatePut_POOL_SIZE = params.MUTATEPUT_POOL_SIZE;
		if(signalBeanList.size() < MutatePut_POOL_SIZE){
			MutatePut_POOL_SIZE = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(MutatePut_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<List<SignalBean>> SplitSignalBeanListList = new ArrayList<List<SignalBean>>();
		
		for(int i=0; i < MutatePut_POOL_SIZE; i++){
			SplitSignalBeanListList.add(new ArrayList<SignalBean>());
		}
		
		int SplitSignalBeanListSize = (signalBeanList.size()/MutatePut_POOL_SIZE)+1;
		
		
		int num=0;
		for(SignalBean signalBean:signalBeanList){
			SplitSignalBeanListList.get(num/SplitSignalBeanListSize).add(signalBean);
			num++;
		}
		
		CountDownLatch latch =new CountDownLatch(MutatePut_POOL_SIZE);
		for(int i=0;i<MutatePut_POOL_SIZE;i++){
			Callable c = new MyCallableput(latch, SplitSignalBeanListList.get(i)); 
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
	
	private class MyCallableput implements Callable<Object> {
		private List<SignalBean> signalBeanList;
		private CountDownLatch latch;
	
		public MyCallableput(CountDownLatch latch,List<SignalBean> signalBeanList) {
			this.latch = latch;
			this.signalBeanList = signalBeanList;
		}
		
		public Object call(){
		
			HbaseDao hbaseDao = new HbaseDao();
			Map<String, HTableInterface> htInterfaces = hbaseDao.MyInit(new String[]{paramUtil.TABLE_NAME,paramUtil.TABLE_NAME_INDEX});

			for(SignalBean signalBean:signalBeanList){
				
				List<String> kafkasignalBean = signalBean.getKafkaValues();
				String mdn = kafkasignalBean.get(0);
				long ts = new TimeFormat().Date2long(kafkasignalBean.get(11));
				Put put = new Put(Bytes.toBytes(kafkasignalBean.get(0)));

				StringBuffer sb =  new StringBuffer();
				for(int i=1;i<13;i++){
					sb.append(kafkasignalBean.get(i)).append(",");
				}
				sb.append(kafkasignalBean.get(13));
				String Value = new String(sb);
				
				put.addColumn(HbaseInput.BYTE_f, HbaseInput.BYTE_rts, ts ,Bytes.toBytes(Value));
				
				hbaseDao.putRow(htInterfaces.get(paramUtil.TABLE_NAME),put);
				
				List<IndexBean> insIndexBeanList = signalBean.getInsIndexBeanList();
				if(insIndexBeanList.size()>0){
					IndexBean indexBean = insIndexBeanList.get(0);
					Put indexput = new Put(Bytes.toBytes(indexBean.getRowkey()));
					indexput.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(mdn), ts ,Bytes.toBytes(indexBean.getValue()));

					
					hbaseDao.putRow(htInterfaces.get(paramUtil.TABLE_NAME_INDEX), indexput);
				}
			}
			hbaseDao.MyBufferFlush(new ArrayList<HTableInterface>(htInterfaces.values()));
			hbaseDao.MyClose(new ArrayList<HTableInterface>(htInterfaces.values()));
			latch.countDown();
			return null;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void HbaseMutatorPut(String table, List<IndexBean> insIndexBeanList){
		ParamUtil params = ParamUtil.getInstance();
		int MutatePut_POOL_SIZE = params.MUTATEPUT_POOL_SIZE;
		if(insIndexBeanList.size() < MutatePut_POOL_SIZE){
			MutatePut_POOL_SIZE = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(MutatePut_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<List<IndexBean>> SplitInsIndexBeanListList = new ArrayList<List<IndexBean>>();
		
		for(int i=0; i < MutatePut_POOL_SIZE; i++){
			SplitInsIndexBeanListList.add(new ArrayList<IndexBean>());
		}
		
		int SplitDelIndexBeanListSize = (insIndexBeanList.size()/MutatePut_POOL_SIZE)+1;
		for(int i=0;i<insIndexBeanList.size();i++){
			SplitInsIndexBeanListList.get(i/SplitDelIndexBeanListSize).add(insIndexBeanList.get(i));
		}
		
		CountDownLatch latch =new CountDownLatch(MutatePut_POOL_SIZE);
		for(int i=0;i<MutatePut_POOL_SIZE;i++){
			Callable c = new MyCallableMutatePutIndex(latch, table, SplitInsIndexBeanListList.get(i)); 
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
	
	private class MyCallableMutatePutIndex implements Callable<Object> {
		private CountDownLatch latch;
		private String table;
		private List<IndexBean> insIndexBeanList;
		
	
		public MyCallableMutatePutIndex(CountDownLatch latch, String table, List<IndexBean> insIndexBeanList) {
			this.latch = latch;
			this.table = table;
			this.insIndexBeanList = insIndexBeanList;
		}
		
		public Object call(){
			BufferedMutator mutator = HbasePool.getMutator(table, ParamUtil.getInstance().MUTATEDEL_WRITEBUFFERSIZE);
			try {
				for(IndexBean indexBean : insIndexBeanList){
					Put put = new Put(Bytes.toBytes(indexBean.getRowkey()));
					put.addColumn(Bytes.toBytes(indexBean.getCf()),Bytes.toBytes(indexBean.getCq()), indexBean.getTs(), Bytes.toBytes(indexBean.getValue()));
					put.setDurability(Durability.SKIP_WAL);
					mutator.mutate(put);
				}
				mutator.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			latch.countDown();
			return null;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void HbaseMutatorPut(Set<SignalBean> signalBeanList){
		ParamUtil params = ParamUtil.getInstance();
		int MutatePut_POOL_SIZE = params.MUTATEPUT_POOL_SIZE;
		if(signalBeanList.size() < MutatePut_POOL_SIZE){
			MutatePut_POOL_SIZE = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(MutatePut_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<List<SignalBean>> SplitSignalBeanListList = new ArrayList<List<SignalBean>>();
		
		for(int i=0; i < MutatePut_POOL_SIZE; i++){
			SplitSignalBeanListList.add(new ArrayList<SignalBean>());
		}
		
		int SplitSignalBeanListSize = (signalBeanList.size()/MutatePut_POOL_SIZE)+1;
		
		
		int num=0;
		for(SignalBean signalBean:signalBeanList){
			SplitSignalBeanListList.get(num/SplitSignalBeanListSize).add(signalBean);
			num++;
		}
		
		CountDownLatch latch =new CountDownLatch(MutatePut_POOL_SIZE);
		for(int i=0;i<MutatePut_POOL_SIZE;i++){
			Callable c = new MyCallableMutatePut(latch, SplitSignalBeanListList.get(i)); 
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
	
	private class MyCallableMutatePut implements Callable<Object> {
		private CountDownLatch latch;
		private List<SignalBean> signalBeanList;
		
	
		public MyCallableMutatePut(CountDownLatch latch, List<SignalBean> signalBeanList) {
			this.latch = latch;
			this.signalBeanList = signalBeanList;
		}
		
		public Object call(){
			BufferedMutator mutator = HbasePool.getMutator(paramUtil.TABLE_NAME, ParamUtil.getInstance().MUTATEPUT_WRITEBUFFERSIZE);
			BufferedMutator mutatorindex = HbasePool.getMutator(paramUtil.TABLE_NAME_INDEX, ParamUtil.getInstance().MUTATEPUT_WRITEBUFFERSIZE);
			
			try {
				for(SignalBean signalBean : signalBeanList){
					List<String> kafkasignalBean = signalBean.getKafkaValues();
					long ts = new TimeFormat().Date2long(kafkasignalBean.get(11));
					Put put = new Put(Bytes.toBytes(kafkasignalBean.get(0)));

					StringBuffer sb =  new StringBuffer();
					for(int i=1;i<13;i++){
						sb.append(kafkasignalBean.get(i)).append(",");
					}
					sb.append(kafkasignalBean.get(13));
					String sbString = new String(sb);
					
					put.addColumn(HbaseInput.BYTE_f, HbaseInput.BYTE_rts, ts ,Bytes.toBytes(sbString));
					
					put.setDurability(Durability.SKIP_WAL);
					mutator.mutate(put);
					
					
					List<IndexBean> insIndexBeanList = signalBean.getInsIndexBeanList();
					if(insIndexBeanList.size()>0){
						IndexBean indexBean = insIndexBeanList.get(0);
						Put indexput = new Put(Bytes.toBytes(indexBean.getRowkey()));
						indexput.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(indexBean.getCq()), ts ,Bytes.toBytes(indexBean.getValue()));

						indexput.setDurability(Durability.SKIP_WAL);
						mutatorindex.mutate(indexput);	
					}
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_signalType    , ts, Bytes.toBytes(kafkasignalBean.get(1)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_area          , ts, Bytes.toBytes(kafkasignalBean.get(2)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_roam          , ts, Bytes.toBytes(kafkasignalBean.get(3)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_roamProv      , ts, Bytes.toBytes(kafkasignalBean.get(4)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_roamCountry   , ts, Bytes.toBytes(kafkasignalBean.get(5)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_lac           , ts, Bytes.toBytes(kafkasignalBean.get(6)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_ci            , ts, Bytes.toBytes(kafkasignalBean.get(7)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_longitude     , ts, Bytes.toBytes(kafkasignalBean.get(8)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_latitude      , ts, Bytes.toBytes(kafkasignalBean.get(9)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_imsi          , ts, Bytes.toBytes(kafkasignalBean.get(10)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_imei          , ts, Bytes.toBytes(kafkasignalBean.get(11)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_currentCiTime , ts, Bytes.toBytes(kafkasignalBean.get(12)));
//					put.addColumn(HbaseInput.BYTE_f, HbaseInput.bytes_lastTime      , ts, Bytes.toBytes(kafkasignalBean.get(13)));
					

				}
				
				
				
				
				mutator.close();
				mutatorindex.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			latch.countDown();
			return null;
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void HbaseMutatorPut2(Set<SignalBean> signalBeanList){
		ParamUtil params = ParamUtil.getInstance();
		int MutatePut_POOL_SIZE = params.MUTATEPUT_POOL_SIZE;
		if(signalBeanList.size() < MutatePut_POOL_SIZE){
			MutatePut_POOL_SIZE = 1;
		}
		ExecutorService pool = Executors.newFixedThreadPool(MutatePut_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<List<SignalBean>> SplitSignalBeanListList = new ArrayList<List<SignalBean>>();
		
		for(int i=0; i < MutatePut_POOL_SIZE; i++){
			SplitSignalBeanListList.add(new ArrayList<SignalBean>());
		}
		
		int SplitSignalBeanListSize = (signalBeanList.size()/MutatePut_POOL_SIZE)+1;
		
		
		int num=0;
		for(SignalBean signalBean:signalBeanList){
			SplitSignalBeanListList.get(num/SplitSignalBeanListSize).add(signalBean);
			num++;
		}
		
		CountDownLatch latch =new CountDownLatch(MutatePut_POOL_SIZE);
		for(int i=0;i<MutatePut_POOL_SIZE;i++){
			Callable c = new MyCallableMutatePut2(latch, SplitSignalBeanListList.get(i)); 
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
	
	private class MyCallableMutatePut2 implements Callable<Object> {
		private CountDownLatch latch;
		private List<SignalBean> signalBeanList;
		
	
		public MyCallableMutatePut2(CountDownLatch latch, List<SignalBean> signalBeanList) {
			this.latch = latch;
			this.signalBeanList = signalBeanList;
		}
		
		public Object call(){
			BufferedMutator mutator = HbasePool.getMutator(paramUtil.TABLE_NAME, ParamUtil.getInstance().MUTATEPUT_WRITEBUFFERSIZE);
//			BufferedMutator mutatorindex = HbasePool.getMutator(paramUtil.TABLE_NAME_INDEX, ParamUtil.getInstance().MutatePut_WriteBufferSize);
			
			try {
				for(SignalBean signalBean : signalBeanList){
					List<String> kafkasignalBean = signalBean.getKafkaValues();
					long ts = new TimeFormat().Date2long(kafkasignalBean.get(11));
					Put put = new Put(Bytes.toBytes(kafkasignalBean.get(0)));

					StringBuffer sb =  new StringBuffer();
					for(int i=1;i<13;i++){
						sb.append(kafkasignalBean.get(i)).append(",");
					}
					sb.append(kafkasignalBean.get(13));
					String sbString = new String(sb);
					
					put.addColumn(HbaseInput.BYTE_f, HbaseInput.BYTE_rts, ts ,Bytes.toBytes(sbString));
					
					put.setDurability(Durability.SKIP_WAL);
					mutator.mutate(put);
					
//					
//					List<IndexBean> insIndexBeanList = signalBean.getInsIndexBeanList();
//					if(insIndexBeanList.size()>0){
//						IndexBean indexBean = insIndexBeanList.get(0);
//						Put indexput = new Put(Bytes.toBytes(indexBean.getRowkey()));
//						indexput.addColumn(HbaseInput.BYTE_f, Bytes.toBytes(indexBean.getCq()), ts ,Bytes.toBytes(indexBean.getValue()));
//
//						indexput.setDurability(Durability.SKIP_WAL);
//						mutatorindex.mutate(indexput);	
//					}

				}
				
				
				
				
				mutator.close();
//				mutatorindex.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			latch.countDown();
			return null;
		}
	}

	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Set<String> HbaseIndexGetter(String tableName,Set<String> lines){
		ExecutorService pool = Executors.newFixedThreadPool(paramUtil.DEFAULT_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		ArrayList<Set<String>> allset = new ArrayList<Set<String>>();
		Set<String> allSetList = new HashSet<String>();
		
		for(int i=0;i<paramUtil.DEFAULT_POOL_SIZE;i++){
			allset.add(new HashSet<String>());
		}
		
		int taskSizeNum = (lines.size()/paramUtil.DEFAULT_POOL_SIZE)+1;

		int linecount=0;
		for(String line:lines){
			allset.get(linecount/taskSizeNum).add(line);
			linecount++;
		}
		
		CountDownLatch latch =new CountDownLatch(paramUtil.DEFAULT_POOL_SIZE);
		for(int i=0;i<paramUtil.DEFAULT_POOL_SIZE;i++){
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
		System.out.println("taskSize"+paramUtil.DEFAULT_POOL_SIZE);
		ExecutorService pool = Executors.newFixedThreadPool(paramUtil.DEFAULT_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		ArrayList<Set<String>> allset = new ArrayList<Set<String>>();
		Map<String, Set<String>> allSetList = new HashMap<String, Set<String>>();
		
		for(int i=0;i<paramUtil.DEFAULT_POOL_SIZE;i++){
			allset.add(new HashSet<String>());
		}
		
		int taskSizeNum = (lines.size()/paramUtil.DEFAULT_POOL_SIZE)+1;

		int linecount=0;
		for(String line:lines){
			allset.get(linecount/taskSizeNum).add(line);
			linecount++;
		}
		CountDownLatch latch =new CountDownLatch(paramUtil.DEFAULT_POOL_SIZE);
		for(int i=0;i<paramUtil.DEFAULT_POOL_SIZE;i++){
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
	public HashMap<String, byte[]> HbaseGetter(String tableName,Set<String> rowKeys){
		ExecutorService pool = Executors.newFixedThreadPool(paramUtil.DEFAULT_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 

		List<String> mdnArrayList = new ArrayList<String>();
		ArrayList<HashSet<String>> allArrayList = new ArrayList<HashSet<String>>();
		Set<HashMap<String, byte[]>> splitMapList = new HashSet<HashMap<String, byte[]>>();
		HashMap<String,byte[]> allMapList = new HashMap<String, byte[]>();

		for(int i=0;i<paramUtil.DEFAULT_POOL_SIZE;i++){
			allArrayList.add(new HashSet<String>());
		}

		for(String rowKey:rowKeys){
			mdnArrayList.add(rowKey.split("\\^")[0]);
		}
		
		Collections.sort(mdnArrayList);

		int taskSizeNum = (mdnArrayList.size()/paramUtil.DEFAULT_POOL_SIZE)+1;
		


		int mdncount=0;
		for(String mdn:mdnArrayList){
			allArrayList.get(mdncount/taskSizeNum).add(mdn);
			mdncount++;
		}

		CountDownLatch latch =new CountDownLatch(paramUtil.DEFAULT_POOL_SIZE);
		for(int i=0;i<paramUtil.DEFAULT_POOL_SIZE;i++){
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
	

}
