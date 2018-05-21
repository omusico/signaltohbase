package com.asiainfo.Bean;

import java.util.ArrayList;
import java.util.List;

public class SignalBeanBak {
	
	private String mdn;
	private String signalType;
	private String area;
	private String roam;
	private String roamProv;
	private String roamCountry;
	private String lac;
	private String ci;
	private String longitude;
	private String latitude;
	private String imsi;
	private String imei;
	private String currentCiTime;
	private String lastTime;
	
	private List<IndexBean> delIndexBeanList = new ArrayList<IndexBean>();
	private List<IndexBean> insIndexBeanList = new ArrayList<IndexBean>();
	private Boolean isGet = false;

	public String getMdn() {
		return mdn;
	}
	public void setMdn(String mdn) {
		this.mdn = mdn;
	}
	public String getSignalType() {
		return signalType;
	}
	public void setSignalType(String signalType) {
		this.signalType = signalType;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public String getRoam() {
		return roam;
	}
	public void setRoam(String roam) {
		this.roam = roam;
	}
	public String getRoamProv() {
		return roamProv;
	}
	public void setRoamProv(String roamProv) {
		this.roamProv = roamProv;
	}
	public String getRoamCountry() {
		return roamCountry;
	}
	public void setRoamCountry(String roamCountry) {
		this.roamCountry = roamCountry;
	}
	public String getLac() {
		return lac;
	}
	public void setLac(String lac) {
		this.lac = lac;
	}
	public String getCi() {
		return ci;
	}
	public void setCi(String ci) {
		this.ci = ci;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public String getCurrentCiTime() {
		return currentCiTime;
	}
	public void setCurrentCiTime(String currentCiTime) {
		this.currentCiTime = currentCiTime;
	}
	public String getLastTime() {
		return lastTime;
	}
	public void setLastTime(String lastTime) {
		this.lastTime = lastTime;
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
	    sb.append(mdn          ).append(",")
		  .append(signalType   ).append(",")
		  .append(area         ).append(",")
		  .append(roam         ).append(",")
		  .append(roamProv     ).append(",")
		  .append(roamCountry  ).append(",")
		  .append(lac          ).append(",")
		  .append(ci           ).append(",")
	      .append(longitude    ).append(",")
		  .append(latitude     ).append(",")
		  .append(imsi         ).append(",")
		  .append(imei         ).append(",")
		  .append(currentCiTime).append(",")
		  .append(lastTime     );
	    if(delIndexBeanList.size()==1){
			  sb.append("\n").append("                 :")
			  .append("del:"+delIndexBeanList.get(0));
	    }
	    if(insIndexBeanList.size()==1){
			  sb.append("\n").append("                 :")
			  .append("ins:"+insIndexBeanList.get(0))
			  
			  ;
	    }

	    
		return new String(sb);
	}
}
