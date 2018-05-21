package com.asiainfo.KafkaDao;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.asiainfo.Util.ParamUtil;

public class KafkaOutputConsole extends KafkaOutput {
	

	
	public <T> void export(List<T> signalList){
		int Get_POOL_SIZE = ParamUtil.GET_POOL_SIZE;
		ExecutorService pool = Executors.newFixedThreadPool(ParamUtil.GET_POOL_SIZE); 
		ArrayList<Future> list = new ArrayList<Future>(); 
		
		List<List<T>> splitSignalBeanSetList = new ArrayList<List<T>>();
		
		for(int i=0; i < Get_POOL_SIZE; i++){
			splitSignalBeanSetList.add(new ArrayList<T>());
		}
		
		int taskSizeNum = (signalList.size()/Get_POOL_SIZE)+1;
		
		int mdncount=0;
		for(T signal:signalList){
			splitSignalBeanSetList.get(mdncount/taskSizeNum).add(signal);
			mdncount++;
		}
		CountDownLatch latch = new CountDownLatch(Get_POOL_SIZE);
		
		for(int i=0;i<Get_POOL_SIZE;i++){
			Callable c = new CallableExport(latch , splitSignalBeanSetList.get(i)); 
			Future f = pool.submit(c);  
			list.add(f);  
		}

		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		pool.shutdown();
	}
	
	private class CallableExport<T> implements Callable<Object> {
		private List<T> signalBeanList;
		private CountDownLatch latch;
		
		public CallableExport(CountDownLatch latch ,List<T> signalBeanList){
			this.latch = latch;
			this.signalBeanList = signalBeanList;
		}
		
		public Object call(){
			for(T signalBean:signalBeanList){
//				try {
//					FileWriter fw = new FileWriter("");
//					fw.write(());
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				logger.info(signalBean);
			}
			latch.countDown();
			return null;
		}
	}

}
