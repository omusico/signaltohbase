package com.asiainfo.Bean;

import java.util.ArrayList;
import java.util.List;

public class SignalBean {

	private List<String> kafkaValues;
	private List<String> hbaseValues;
	
	private List<IndexBean> delIndexBeanList = new ArrayList<IndexBean>();
	private List<IndexBean> insIndexBeanList = new ArrayList<IndexBean>();
	private Boolean isGet = false;

	public List<String> getKafkaValues() {
		return kafkaValues;
	}
	public void setKafkaValues(List<String> kafkaValues) {
		this.kafkaValues = kafkaValues;
	}
	public void clearKafkaValues(){
		if(kafkaValues!=null){
			kafkaValues.clear();
		}
	}
	public List<String> getHbaseValues() {
		return hbaseValues;
	}
	public void setHbaseValues(List<String> hbaseValues) {
		this.hbaseValues = hbaseValues;
	}
	public void clearHbaseValues(){
		if(hbaseValues!=null){
			hbaseValues.clear();	
		}
	}
	
	public List<IndexBean> getDelIndexBeanList() {
		return delIndexBeanList;
	}
	public void setDelIndexBeanList(List<IndexBean> delIndexBeanList) {
		this.delIndexBeanList = delIndexBeanList;
	}
	public List<IndexBean> getInsIndexBeanList() {
		return insIndexBeanList;
	}
	public void setInsIndexBeanList(List<IndexBean> insIndexBeanList) {
		this.insIndexBeanList = insIndexBeanList;
	}
	public Boolean getIsGet() {
		return isGet;
	}
	public void setIsGet(Boolean isGet) {
		this.isGet = isGet;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		
		sb.append("kafkavalue :");
		for(int i=0;i<13;i++){
			sb.append(kafkaValues.get(i)).append(",");
		}
		sb.append(kafkaValues.get(13));
		
		sb.append("\n").append("hbasevalue :");
		if(hbaseValues!=null){
			for(int i=0;i<13;i++){
				sb.append(hbaseValues.get(i)).append(",");
			}
			sb.append(hbaseValues.get(13));
		}
	    if(delIndexBeanList.size()==1){
			  sb.append("\n").append("           :")
			  .append("del:"+delIndexBeanList.get(0));
	    }
	    if(insIndexBeanList.size()==1){
			  sb.append("\n").append("           :")
			  .append("ins:"+insIndexBeanList.get(0))
			  
			  ;
	    }

	    
		return new String(sb);
	}
	
	public String getkafka(){
		StringBuffer sb = new StringBuffer();
		
		sb.append("kafkavalue :");
		for(int i=0;i<13;i++){
			sb.append(kafkaValues.get(i)).append(",");
		}
		sb.append(kafkaValues.get(13));
		return new String(sb);
	}
}
