package com.asiainfo.Bean;

public class IndexBean {

	private String table;
	private String rowkey;
	private String cf;
	private String cq;
	private long ts;

	private String value;
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String getCf() {
		return cf;
	}
	public void setCf(String cf) {
		this.cf = cf;
	}
	public String getCq() {
		return cq;
	}
	public void setCq(String cq) {
		this.cq = cq;
	}
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
	    sb.append(table        ).append(",")
		  .append(rowkey   ).append(",")
		  .append(cf         ).append(",")
		  .append(cq         ).append(",")
		  .append(ts     )
		  ;
	    
		return new String(sb);
	}
}
