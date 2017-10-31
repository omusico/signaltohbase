package com.asiainfo.Main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.net.ntp.TimeStamp;

import com.asiainfo.HbaseDao.HbaseInput;
import com.google.common.collect.Sets;

class num{
	public static int addsize = 0;
	public static int addsizetmp = 0;
	public static int getsize = 0;
}

public class ArrayListInThread implements Runnable {

    public  static final Lock lock  = new ReentrantLock();
    public  static final Condition notFull = lock.newCondition();
    public  static final Condition notEmpty= lock.newCondition();
	static List<String> idCounter = Collections.synchronizedList(new ArrayList<String>());// not thread safe
	//static List<String> idCounter = (new ArrayList<String>());
	//Set<String> idCounter = Sets.newConcurrentHashSet();
    
	public void take(){
		while(true){
		lock.lock();
		try {
		while(idCounter.size()>=1000){
			//System.out.println(idCounter.size());

			num.addsize+=num.addsizetmp;
			num.getsize+=idCounter.size();
			if(num.getsize!=num.addsize){
				System.err.println(num.getsize+" "+num.addsize+"!!!!!!!!!!!!");
			}
			//num.getsize=0;num.addsize=0;
        	for(String a:idCounter){
        		System.out.println("get : "+a);
        	}
        	//System.out.println("+++++++++++++++++++++");
			idCounter.clear();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			notFull.signalAll();
			
		}

			notEmpty.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
        	lock.unlock();
        }
		}
	}
	
	
//    List<String> list1 = Collections.synchronizedList(new ArrayList<String>());// thread safe
    public void run() {
    	int i=0;
    	while(true){
            try {
                Thread.sleep((int)(Math.random()));
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            
            lock.lock();
        	try {
	            while(idCounter.size()>=1000){

	            	//System.out.println("insert size : "+idCounter.size());
	            	num.addsizetmp=idCounter.size();
	            	notEmpty.signalAll();
	            	notFull.await();
					
	            }
            } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	finally{
            	lock.unlock();
            }
            i++;
            System.out.println("add : "+Thread.currentThread().getName()+":"+String.valueOf(i));
            idCounter.add(Thread.currentThread().getName()+":"+String.valueOf(i));
    	}

    }

    public static void main(String[] args) throws InterruptedException {
    	long t1 = System.currentTimeMillis();  
        ThreadGroup group = new ThreadGroup("mygroup");
        Thread consumer = new Thread(group, new DataConsumer1(), "consumer");
        consumer.start();
        ArrayListInThread t = new ArrayListInThread();
        new DataConsumer1().start();
        for (String i :new String[]{"a","b","c","d"}) {
        	for(int a=0;a<25;a++){
                Thread th = new Thread(group, t, i+a);
                th.start();
        	}
        }
        
//        while (group.activeCount() > 0) {
//            Thread.sleep(10);
//        }
        //System.out.println(idCounter.size()); // it should be 10000 if thread safe collection is used.
      //long t2 = System.currentTimeMillis();  
      //  System.out.println(t2-t1);
    }
}

class DataConsumer1 extends Thread{
    DataConsumer1(){
    }
    @Override
    public void run() {
        while(true){
            new ArrayListInThread().take();
        }
    }   
}