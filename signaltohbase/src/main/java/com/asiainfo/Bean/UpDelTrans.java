package com.asiainfo.Bean;

import java.util.ArrayList;
import java.util.List;

public class UpDelTrans {
	
	//upValue:{mdn,value,roam|local}
	private String[] upValue = new String[3];	
	private List<String[]> delArrayList = new ArrayList<String[]>();
	
	public String[] getUpValue() {
		return upValue;
	}
	public void setUpValue(String[] upValue) {
		this.upValue = upValue;
	}
	public List<String[]> getDelArrayList() {
		return delArrayList;
	}
	public void setDelArrayList(List<String[]> delArrayList) {
		this.delArrayList = delArrayList;
	}

}
