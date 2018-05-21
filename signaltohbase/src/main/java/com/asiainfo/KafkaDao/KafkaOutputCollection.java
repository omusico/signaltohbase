package com.asiainfo.KafkaDao;

import java.util.Arrays;

import com.asiainfo.Bean.SignalBean;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.Main.HbaseMain;
import com.asiainfo.Util.ParamUtil;

public class KafkaOutputCollection extends KafkaOutput {

//	@Override
//	public void write(String signalFormat) {
//		SignalBean signalBean = new SignalBean();
//		signalBean.setKafkaValues(Arrays.asList(signalFormat.split(",",-1)));					
//		HbaseMain.lock.lock();
//        try {	
//			while(HbaseInput.signalBeanList.size() >= ParamUtil.size_perbatch){
//                HbaseMain.notEmpty.signalAll();//唤醒消费线程
//                HbaseMain.notFull.await();//阻塞生产线程
//			}
//			HbaseInput.signalBeanList.add(signalBean);
//        }  catch (InterruptedException e) {
//			e.printStackTrace();
//		}finally{
//	    	HbaseMain.lock.unlock();	
//        }
//	}

}
