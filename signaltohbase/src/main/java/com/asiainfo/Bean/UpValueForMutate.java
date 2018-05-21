package com.asiainfo.Bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpValueForMutate {

	private List<UpDelTrans> updelTransList = new ArrayList<UpDelTrans>(); 
	private Map<String, String> finLocalDataMap = new HashMap<String,String>();
	private Map<String, String> finRoamDataMap = new HashMap<String,String>();
	
	public List<UpDelTrans> getUpdelTransList() {
		return updelTransList;
	}
	public void setUpdelTransList(List<UpDelTrans> updelTransList) {
		this.updelTransList = updelTransList;
	}
	public Map<String, String> getFinLocalDataMap() {
		return finLocalDataMap;
	}
	public void setFinLocalDataMap(Map<String, String> finLocalDataMap) {
		this.finLocalDataMap = finLocalDataMap;
	}
	public Map<String, String> getFinRoamDataMap() {
		return finRoamDataMap;
	}
	public void setFinRoamDataMap(Map<String, String> finRoamDataMap) {
		this.finRoamDataMap = finRoamDataMap;
	}
}
