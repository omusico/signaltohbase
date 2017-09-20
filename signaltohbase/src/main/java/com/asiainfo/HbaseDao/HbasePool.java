package com.asiainfo.HbaseDao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HbasePool {

	public static HConnection hConnection = null;
	private static Configuration configuration = null;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.0.30,192.168.0.31,192.168.0.21,192.168.0.22,192.168.0.23");
		configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
	}

	public static HConnection getHTablePool() throws IOException {
		if (hConnection == null) {
			hConnection = HConnectionManager.createConnection(configuration);
		}
		return hConnection;
	}

	public static synchronized HTableInterface getHtable(){
		return null;
	}
	
	
	public static synchronized HTableInterface getHtable(String tableName) {
		try {
			HTableInterface tableInterface = null;
			HConnection hConnection = HbasePool.getHTablePool();
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
