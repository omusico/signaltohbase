package com.asiainfo.Main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.asiainfo.Bean.SignalBean;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.KafkaDao.KafkaMyConsumerOld;
import com.asiainfo.Util.DataFormat;


public class HbaseMainTest {
	private static Logger logger = Logger.getLogger(HbaseMainTest.class);
	public static void main(String[] args) throws IOException {
		new HbaseMainTest().doTest2();
	}
	
	public void doTest(){
		try {
			HbaseMainOld.timeList.add(System.currentTimeMillis());
			HbaseMainOld.timeList.add(System.currentTimeMillis());
			HbaseMainOld.timeList.add(System.currentTimeMillis());
			HbaseMainOld.timeList.add(System.currentTimeMillis());
			BufferedReader br = new BufferedReader(new FileReader(new File(System.getProperty("user.dir")+"/test/data.txt")));
			String line = br.readLine();
		
			while(line!=null){
			

				String[] lines = line.split(",",-1);
				SignalBean signalBean = new SignalBean();
				
				signalBean.setKafkaValues(Arrays.asList(lines));
			    
				HbaseInput.signalBeanList.add(signalBean);
				if(HbaseInput.signalBeanList.size() >= 4){
					new HbaseInput().HbaseSignalPut(HbaseInput.signalBeanList);
				}
				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void doTest2(){
		String line = "18668186910,CsMap[0-11],574,510,034,CHN,,,460016740104321,,,20171206092018,,";
		String line2 = "18668186910,PsRoam[0-11],571,,034,CHN,,,460017155925105,356978069630751,,20171206092021,,";	
		
		String[] lines = line.split(",",-1);
		SignalBean signalBean = new SignalBean();
		signalBean.setKafkaValues(Arrays.asList(lines));

		
		String[] line2s = line2.split(",",-1);
		signalBean.setHbaseValues(Arrays.asList(line2s));

	    
	    System.out.println(new DataFormat().compareSignalBean(signalBean));
			
	}
	
	public void doTestKafka(String topicname){
		new KafkaMyConsumerOld(topicname, 2).start(); 
	}
	
	public void log(){
		logger.info("consumer start input to hbase!");
	}

}
