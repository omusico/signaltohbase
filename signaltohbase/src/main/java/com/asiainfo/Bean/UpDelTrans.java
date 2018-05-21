package com.asiainfo.Bean;

import java.util.ArrayList;
import java.util.List;

public class UpDelTrans {
	
	//upValue:{mdn,value,roam|local}
	private SignalBean signalBean = new SignalBean();	
	private List<String[]> delArrayList = new ArrayList<String[]>();
	
	public SignalBean getSignalBean() {
		return signalBean;
	}
	public void setSignalBean(SignalBean signalBean) {
		this.signalBean = signalBean;
	}
	public List<String[]> getDelArrayList() {
		return delArrayList;
	}
	public void setDelArrayList(List<String[]> delArrayList) {
		this.delArrayList = delArrayList;
	}

}
