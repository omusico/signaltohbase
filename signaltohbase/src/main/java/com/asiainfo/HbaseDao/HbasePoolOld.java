package com.asiainfo.HbaseDao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*; 
import org.apache.hadoop.hbase.client.*; 

public class HbasePoolOld {

	public static HConnection hConnection = null;
	private static Configuration configuration = null;

	static {
		configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","ocdc-dn-03,ocdc-ser-01,ocdc-dn-22");  
        configuration.set("hbase.zookeeper.property.clientPort","2181");  
        configuration.set("zookeeper.znode.parent","/hbase-unsecure");  
	}

	public static HConnection getHTablePool() throws IOException {
		if (hConnection == null) {
			hConnection = HConnectionManager.createConnection(configuration);  
		}
		return hConnection;
	}

//	public static synchronized HTableInterface getHtable(){
//		return null;
//	}
	
	
	public static synchronized HTableInterface getHtable(String tableName) {
		try {
			HTableInterface tableInterface = null;
			HConnection hConnection = HbasePoolOld.getHTablePool();
			if (hConnection == null) {
				System.out.println("no connection");
			} else {
				tableInterface = hConnection.getTable(tableName);
			}
			return tableInterface;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void closePool() {
		try {
			hConnection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Configuration getConfigration() {
		return configuration;
	}

}
