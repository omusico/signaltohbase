package com.asiainfo.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import com.asiainfo.KafkaDao.KafkaMyProducer;
import com.asiainfo.KafkaDao.KafkaSignaConsumerZbRoamput;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsLoc;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsMap;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsSms;
import com.asiainfo.KafkaDao.KafkaSignalConsumerCsVoc;
import com.asiainfo.KafkaDao.KafkaSignalConsumerPs;
import com.asiainfo.KafkaDao.KafkaSignalConsumerPsRoam;
import com.asiainfo.Util.Constants;
import com.asiainfo.Util.DataFormat;

public class TestMain {

	public static void main(String[] args) {

		
		

	}
	
	private void testa(){
		String sms = 
				"20180517155504,8613010476500,460018655088164,860212031476770,13018843376,,,18320957849,6,0,251,0,16048,8542,0,0,0,102,35195,20180517155508,1,460,121,12102,1210212,NBBSC18HWE_NBB3414YX273GH,121.4794810000000000,29.8738910000000000,4,,13816,3558,576,16048,8541,2,2";
	    System.out.println(new KafkaSignalConsumerCsSms("a",1).signalFormat(sms));
	    
	    String loc =
	    		"20180517155413,13128554649,460012249925427,8610240240574778,3,0,0,1,65534,65535,16912,21409,2,255,20180517155413,1,460,,,,,,,,,,,,,,,121,12104,1210481,JXBSC66HWE_JXB5172HNYCYDGG,120.3797100000000000,30.3561200000000000,现网运行状态,,,乡村,0,现网运行状态,155,1,13651,3558,756,16912,21409,2";
	    System.out.println(new KafkaSignalConsumerCsLoc("a",1).signalFormat(loc));
	    
	    String voc = 
	    		"20180517155259,460014676066552,,13064613686,,,13655897592,9,7,12,0,255,55176,3281,55176,3281,0,2,255,255,255,255,255,16,1612273,38747,20180517155558,2,255,460,121,12107,1210782,JHRNC08.328,120.0953810000000000,29.3215110000000000,0,,121,12107,1210782,JHRNC08.328,120.0953810000000000,29.3215110000000000,13817,6526,000,55176,3281,0,2";
	    System.out.println(new KafkaSignalConsumerCsVoc("a",1).signalFormat(voc));
	    
	    String ps = 
	    		"15557990161^460017995408243^867439034269920^6^460^01^^^1526543758^817276^31^-4611686018401022206^579^579^1210783^120.3164340000000000^29.1429460000000000^817276^11^7^5787^C787C1F^3gnet^1^2";
	    System.out.println(new KafkaSignalConsumerPs("a",1).signalFormat(ps));
	    
	    String psroam = 
	    		"18658735998^460018735603997^864083035974071^1^460^01^220.206.182.89^1^1526443308^55662^5421^4611686018488664834^577^591^^^^^^1^D96E^152D^3gnet^1^038^CHN^";
	    System.out.println(new KafkaSignalConsumerPsRoam("a",1).signalFormat(psroam));
	    
	    String csmap = 
	    		"20180516123720,8618658735998,18658735998,460018735603997,864083035974070,gmapmm,3,1,65535,861569577000,8613254543,20180516123720,CHN,036,577,CHN,038,593,CHN,036,577,1";
	    System.out.println(new KafkaSignalConsumerCsMap("a",1).signalFormat(csmap));
	    
	    String zbroam = 
	    		"13059879933^0202^20180517155739^030^V0342100^036^V0330900^01^E02^E020005^20180517155613^558^580";
	    System.out.println(new KafkaSignaConsumerZbRoamput("a",1).signalFormat(zbroam));
	}
	
	private void testb(String[] args){
		List<String> msgSet = new ArrayList<String>();
      FileReader fr=null;
		try {
//			fr = new FileReader("/data03/test/llw/RealTimeSignalTest2/data/cssms/cssms_201805101046.txt");
			fr = new FileReader(args[1]);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		BufferedReader bf = new BufferedReader(fr);
      String msg = null;
		try {
			while ((msg=bf.readLine())!=null){
				msgSet.add(msg);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		KafkaMyProducer kafkaMyProducer = new KafkaMyProducer(args[0],msgSet);
		kafkaMyProducer.setSend();
	}
}
