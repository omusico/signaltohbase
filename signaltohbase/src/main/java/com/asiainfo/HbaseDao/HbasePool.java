package com.asiainfo.HbaseDao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;

import com.asiainfo.Util.ParamUtil;

public class HbasePool {
	
	volatile private static Connection connection = null;
	public static Configuration configuration = null;

	static {
		ParamUtil paramUtil = ParamUtil.getInstance();
		configuration = HBaseConfiguration.create();		
		configuration.set("hbase.zookeeper.quorum", paramUtil.HBASE_ZOOKEEPER_QUORUM);
		configuration.set("zookeeper.znode.parent", paramUtil.ZOOKEEPER_ZNODE_PARENT);
	}
	
	public static Connection getHConn() throws IOException {
		if(connection!=null){
			
		}else{
			synchronized(HbasePool.class){
				if(connection==null){
					connection = ConnectionFactory.createConnection(configuration);
				}
			}
		}
		return connection;
	}
	
	public synchronized static Table getHtable(String tableName) {
		try {
			Table table = null;
			Connection connection = HbasePool.getHConn();
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
	
	public static BufferedMutator getMutator(String tableName,long writeBufferSize) {
		try {
			BufferedMutator mutator  = null;
			Connection connection = HbasePool.getHConn();

			final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {  
	            @Override  
	            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {  
	                for (int i = 0; i < e.getNumExceptions(); i++) {  
	                    System.out.println("Failed to sent put " + e.getRow(i) + ".");  
	                }  
	            }  
	        };  
	        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(listener);  
	        params.writeBufferSize(writeBufferSize);
			mutator = connection.getBufferedMutator(params);
			return mutator;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static BufferedMutator getMutator(String tableName) {
		try {
			BufferedMutator mutator  = null;
			Connection connection = HbasePool.getHConn();

			final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {  
	            @Override  
	            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {  
	                for (int i = 0; i < e.getNumExceptions(); i++) {  
	                    System.out.println("Failed to sent put " + e.getRow(i) + ".");  
	                }  
	            }  
	        };  
	        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(listener);  
			mutator = connection.getBufferedMutator(params);
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
