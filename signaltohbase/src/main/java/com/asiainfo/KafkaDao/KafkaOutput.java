package com.asiainfo.KafkaDao;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.ParamUtil;

public class KafkaOutput {
	private static Logger logger = Log4JUtil.getLogger();
	public static List<String> signalFormatList = new ArrayList<String>();

	public void fillCollToInput(String formatedSignal) {
		HbaseMain.lock.lock();
        try {	
			while(signalFormatList.size() >= ParamUtil.INPUTBATCHSIZE){
                HbaseMain.notEmpty.signalAll();//唤醒消费线程
                HbaseMain.notFull.await();//阻塞生产线程
			}
			signalFormatList.add(formatedSignal);
        }  catch (InterruptedException e) {
			e.printStackTrace();
		}finally{
	    	HbaseMain.lock.unlock();	
        }
	}
	
    public void readyForInput(List<?> signalFormatList) {  
      HbaseMain.lock.lock();
        try {  
            while (signalFormatList.size() >= ParamUtil.INPUTBATCHSIZE ){//如果队列满了
            	Log4JUtil.addNowts();
            	logger.debug(ParamUtil.LOGBATCHSIZE + " signal batch cost " + Log4JUtil.getTimeInterval() +" s");
        		new KafkaOutput().exeInput(signalFormatList);
				//new TestHbaseImport().HbaseSignalPut(signalFormatList);

        		
        		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        		Thread.currentThread().sleep(ParamUtil.SLEEPSEC*1000);
        		
        		logger.debug("awake hbase input thread");
            	HbaseMain.notFull.signalAll();//唤醒生产线程
            }
            HbaseMain.notEmpty.await();//阻塞消费线程
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
        HbaseMain.lock.unlock();
        }
    }
    
    public <T> void exeInput(List<T> signalFormatList){
		int Get_POOL_SIZE = ParamUtil.GET_POOL_SIZE;
		ExecutorService pool = Executors.newFixedThreadPool(ParamUtil.GET_POOL_SIZE); 
		ArrayList<Future<?>> futureList = new ArrayList<Future<?>>(); 
		
		List<List<T>> splitSignalBeanSetList = new ArrayList<List<T>>();
		
		for(int i=0; i < Get_POOL_SIZE; i++){
			splitSignalBeanSetList.add(new ArrayList<T>());
		}
		
		int taskSizeNum = (signalFormatList.size()/Get_POOL_SIZE)+1;
		
		int mdncount=0;
		for(T signal:signalFormatList){
			splitSignalBeanSetList.get(mdncount/taskSizeNum).add(signal);
			mdncount++;
		}
		CountDownLatch latch = new CountDownLatch(Get_POOL_SIZE);
		
		for(int i=0;i<Get_POOL_SIZE;i++){
			Callable c = new CallableExport(latch , splitSignalBeanSetList.get(i)); 
			Future f = pool.submit(c);  
			futureList.add(f);  
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
				System.out.println(signalBean);
			}
			latch.countDown();
			return null;
		}
	}

}
