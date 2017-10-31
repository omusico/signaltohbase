package com.asiainfo.Bean;

import java.util.HashMap;

public class GetResultMaps {

	private HashMap<String, byte[]> resultMap = new HashMap<String, byte[]>();
	private HashMap<String, byte[]> roamResultMap = new HashMap<String, byte[]>();
	
	public HashMap<String, byte[]> getResultMap() {
		return resultMap;
	}
	public void setResultMap(HashMap<String, byte[]> resultMap) {
		this.resultMap = resultMap;
	}
	public HashMap<String, byte[]> getRoamResultMap() {
		return roamResultMap;
	}
	public void setRoamResultMap(HashMap<String, byte[]> roamResultMap) {
		this.roamResultMap = roamResultMap;
	}
}
