package com.asiainfo.Test;

import java.util.List;

import org.apache.log4j.Logger;

import com.asiainfo.Main.HbaseMainOld;
import com.asiainfo.Util.Log4JUtil;
import com.asiainfo.Util.ParamUtil;

public class TestHbaseImport{
	private static Logger logger = Log4JUtil.getLogger();
    public void take(List<?> signalList) {  
        HbaseMainOld.lock.lock();
        try {  
            while (signalList.size() >= ParamUtil.INPUTBATCHSIZE ){//如果队列满了
        		new TestExportToLocal().export(signalList);
        		
//            	new TestHbaseImport().HbaseSignalPut(signalList);
            	
            	HbaseMainOld.notFull.signalAll();//唤醒生产线程
            }
            HbaseMainOld.notEmpty.await();//阻塞消费线程
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
        	HbaseMainOld.lock.unlock();
        }
    }
    
	public void HbaseSignalPut(List<?> signalList) {
		for(Object listObject : signalList){
			if(!(listObject==null || "".equals(listObject) )){
				logger.info(listObject);
			}
		}
	}
	
	
}
