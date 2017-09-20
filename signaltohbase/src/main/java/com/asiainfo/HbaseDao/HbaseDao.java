package com.asiainfo.HbaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDao {
	public static Configuration conf = HBaseConfiguration.create();
	public static ArrayList<Put> puts = new ArrayList<Put>();
	
	public final static String TABLE_NAME = "ZJLT:signal";
	public final static String TABLE_NAME_INDEX = "ZJLT:signalindex";
	public final static long WRITEBUFFERSIZE = 3*1024*1024;

	static {
		conf.set("hbase.zookeeper.quorum", "192.168.0.30,192.168.0.31,192.168.0.21,192.168.0.22,192.168.0.23");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
	}

	
	public HTableInterface[] MyInit(String[] names){
		HTableInterface MyhTable = null;
		HTableInterface MyhTable_index = null;
		try {
			if(names.length==2){
				MyhTable = HbasePool.getHtable(names[0]);
				MyhTable.setAutoFlush(false,false);
				MyhTable.setWriteBufferSize(WRITEBUFFERSIZE);
				
				MyhTable_index = HbasePool.getHtable(names[1]);
				MyhTable_index.setAutoFlush(false,false);
				MyhTable_index.setWriteBufferSize(WRITEBUFFERSIZE);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new HTableInterface[]{MyhTable,MyhTable_index};
	}
	
	
	public void MyBufferFlush(HTableInterface[] htInterfaces){
		try {
			htInterfaces[0].flushCommits();
			htInterfaces[1].flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void MyClose(HTableInterface[] htInterfaces){
		try {
			htInterfaces[0].close();
			htInterfaces[1].close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String tablename, String[] columnFamilies)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("Table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			for (String columnFamily : columnFamilies) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(tableDesc);

			if (admin.tableExists(tablename)) {
				System.out.println("create table success!");
			} else {
				System.out.println("create table failed!");
			}
		}
		admin.close();
	}

	public void putRow(HTableInterface htable, String rowKey, String columnFamily, String key, String value, long ts) {
		try {
			Put rowPut = new Put(Bytes.toBytes(rowKey));
			rowPut.add(columnFamily.getBytes(), key.getBytes(), ts, value.getBytes());
			rowPut.setDurability(Durability.SKIP_WAL);
			htable.put(rowPut);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void putRow(HTableInterface htable, String rowKey, String columnFamily, String key, String value) {
		long ts = System.currentTimeMillis();
		putRow(htable, rowKey, columnFamily, key, value, ts);
	}
	
	public Result getRow(HTableInterface table, String rowKey) {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = null;
		try {
			result = table.get(get);
			result.listCells();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public Map<String, Long> getSizeBatch(String index,List<String> lacciList){
		HTableInterface table = HbasePool.getHtable(index);
		byte[] family = Bytes.toBytes("f");
		List<Get> getList = new ArrayList<Get>();
		for(String lacci:lacciList){
			getList.add(new Get(Bytes.toBytes(lacci)).addFamily(family));
		}
		Map<String, Long> SectionResult = new HashMap<String,Long>();
		try {
			Result[] results = table.get(getList);
			for(Result result:results){
				if(result.getRow()!=null){
					SectionResult.put(new String(result.getRow()),new Long((long)result.size())); 
				}
			}
			for(String lacci:lacciList){
				if(!SectionResult.containsKey(lacci)){
					SectionResult.put(lacci,0l); 
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return SectionResult;
	}
	
	public long getsize(String name,String rowKey){
		HTableInterface table = HbasePool.getHtable(name);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes("f"));
		Result result = null;
		try {
			result = table.get(get);
			return (result.listCells().size());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public Map<String, byte[]> getRowBatch(String tableName, Set<String> rowKeys) {
		HTableInterface htable = HbasePool.getHtable(tableName);
		ArrayList<Get> GetList = new ArrayList<Get>();
		Map<String, byte[]> ResultMap = new LinkedHashMap<String, byte[]>();
		Result[] results = null;
		for (String rowKey : rowKeys) {
			GetList.add(new Get(Bytes.toBytes(rowKey.split("\\^")[0])));
		}
		try {
			results = htable.get(GetList);
			htable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		int i = 0;
		for(String rowKey:rowKeys){
			ResultMap.put(rowKey.split("\\^")[0],results[i].getValue(HbaseInput.F_byte, HbaseInput.RTS_byte));
			i++;
		}
		return ResultMap;
	}
	
	

	public void delete(String tableName, ArrayList<Delete> deletes) {
		try {
			HTableInterface htable = HbasePool.getHtable(tableName);
			htable.delete(deletes);
			htable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void delete(HTableInterface table, String rowKey, String family, String qualifier, String timestamp) {
		try {
			// 准备删除数据
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			if (family != null && qualifier != null) {
				delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			} else if (family != null && qualifier == null) {
				delete.deleteFamily(Bytes.toBytes(family));
			}
			// 检查时间戳
			if (timestamp != null) {
				delete.setTimestamp(Long.parseLong(timestamp));
			}
			// 进行数据删除

			table.delete(delete);
		} catch (Exception e) {

		}
	}

	public ResultScanner getRow_SubstringComparator(HTable table, String cf, String column, String Substring)
			throws IOException {
		SubstringComparator comp = new SubstringComparator(Substring);

		SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), column.getBytes(), CompareOp.EQUAL,
				comp);
		Scan scan = new Scan();
		scan.setFilter(filter);

		ResultScanner rs = table.getScanner(scan);
		return rs;
	}

	// public static void main(String[] args) throws IOException {
	//// putRow(hTable, "123456","all","time","111,222,333");
	// HTable hTable = new HTable(conf, "ZJLT:test");
	// Result result = new HbaseDao().getRow(hTable,"123456");
	// byte[] bt = result.getValue("all".getBytes(), "time".getBytes());
	// System.out.println(result.isEmpty());
	// System.out.println("!"+new String(bt)+"!");
	// }

}
