package com.asiainfo.HbaseDao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class HbasePool {
	
	public static Connection connection = null;
	private static Configuration configuration = null;

	static {
		configuration = HBaseConfiguration.create();		
		configuration.set("hbase.zookeeper.quorum", "ocdc-dn-01,ocdc-dn-02,ocdc-dn-03,ocdc-dn-04,ocdc-dn-05");
		configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
	}
	
	public static Connection getHTablePool() throws IOException {
		if (connection == null) {
			connection = ConnectionFactory.createConnection(configuration);  
		}
		return connection;
	}
	
	public static synchronized Table getHtable(String tableName) {
		try {
			Table table = null;
			Connection connection = HbasePool.getHTablePool();
			if (connection == null) {
				System.out.println("no connection");
			} else {
				table = connection.getTable(TableName.valueOf(tableName));
			}
			return table;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static synchronized BufferedMutator getMutator(String tableName) {
		try {
			BufferedMutator mutator  = null;
			Connection hConnection = HbasePool.getHTablePool();
			if (hConnection == null) {
				System.out.println("no connection");
			} else {
				mutator = hConnection.getBufferedMutator(TableName.valueOf(tableName));
			}
			return mutator;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}


	public static void closePool() {
		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Configuration getConfigration() {
		return configuration;
	}

}
