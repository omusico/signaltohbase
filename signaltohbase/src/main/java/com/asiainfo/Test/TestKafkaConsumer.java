package com.asiainfo.Test;

import java.util.ArrayList;
import java.util.List;

import com.asiainfo.KafkaDao.*;
import com.asiainfo.Util.ParamUtil;
import com.asiainfo.Util.Log4JUtil;

public class TestKafkaConsumer {
	
	public static List<String> signalFormatList = new ArrayList<String>();
	
	
	public static void main(String[] args) {
		new TestKafkaConsumer().exportToFile(args[0]);
	}
	
	public void exportToFile(String signal){
		KafkaSignalConsumer.InitKafkaOutput(new KafkaOutputConsole());
		new TestDataConsumer(signalFormatList).start();
		switch (signal) {
		case "csloc":
			new KafkaSignalConsumerCsLoc (ParamUtil.TOPIC_CS_LOC_SIGNAL, 2).start();
			break;
		case "csvoc":
			new KafkaSignalConsumerCsVoc (ParamUtil.TOPIC_CS_VOC_SIGNAL, 2).start();
			break;
		case "cssms":
			new KafkaSignalConsumerCsSms (ParamUtil.TOPIC_CS_SMS_SIGNAL, 2).start();
			break;
		case "csmap":
			new KafkaSignalConsumerCsMap (ParamUtil.TOPIC_MAP_DEPOSIT_SIGNAL,2).start();
			break;
		case "ps":
			new KafkaSignalConsumerPs    (ParamUtil.TOPIC_PS_SIGNAL, 6).start();
			break;
		case "psroam":
			Log4JUtil.LOGGER4JFILE = "psroamlog4j.properties";
			new KafkaSignalConsumerPsRoam(ParamUtil.TOPIC_PS_ROAM_SIGNAL,6).start();
			break;
		case "zbroamout":
			Log4JUtil.LOGGER4JFILE = "zbroamoutlog4j.properties";
			new KafkaSignaConsumerZbRoamput(ParamUtil.TOPIC_ZB_ROAMOUT_SIGNAL,2).start();
			break;
		case "all":
			new KafkaSignalConsumerCsLoc (ParamUtil.TOPIC_CS_LOC_SIGNAL, 2).start();
			new KafkaSignalConsumerCsVoc (ParamUtil.TOPIC_CS_VOC_SIGNAL, 2).start();
			new KafkaSignalConsumerCsSms (ParamUtil.TOPIC_CS_SMS_SIGNAL, 2).start();
			new KafkaSignalConsumerPs    (ParamUtil.TOPIC_PS_SIGNAL, 6).start();
//			new KafkaSignalConsumerCsMap (ParamUtil.TOPIC_MAP_DEPOSIT_SIGNAL,2).start();
//			new KafkaSignalConsumerPsRoam(ParamUtil.TOPIC_PS_ROAM_SIGNAL,6).start();
			break;
		default:
			break;
		} 

	}
}


class TestDataConsumer extends Thread{
	private List<?> signalList;
	TestDataConsumer(List<?> signalList){
		this.signalList = signalList;
    }
    @Override
    public void run() {
        while(true){
            new TestHbaseImport().take(signalList);
        }
    }   
}