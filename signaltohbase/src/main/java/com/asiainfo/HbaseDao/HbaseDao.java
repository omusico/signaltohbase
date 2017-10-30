package com.asiainfo.HbaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes; 

public class HbaseDao {
	public static Configuration conf = HBaseConfiguration.create();
	public static ArrayList<Put> puts = new ArrayList<Put>();
	
	public final static String TABLE_NAME = "ZJBDP:signal";
	public final static String TABLE_NAME_INDEX = "ZJBDP:signalindex";
	public final static String TABLE_ROAM_NAME = "ZJBDP:roamsignal";
	public final static String TABLE_ROAM_NAME_INDEX = "ZJBDP:roamsignalindex";
	public final static long WRITEBUFFERSIZE = 5*1024*1024;
	


	static {
		conf.set("hbase.zookeeper.quorum", "ocdc-dn-03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
	}

	
	public Map<String,HTableInterface> MyInit(String[] tablenames){
		HashMap<String, HTableInterface> MyhTables = new HashMap<String, HTableInterface>();

		for(String tablename:tablenames){
			HTableInterface MyhTable = null;
			if(tablename.length()>0){
				MyhTable = HbasePoolOld.getHtable(tablename);
				MyhTable.setAutoFlush(false,false);
				try {
					MyhTable.setWriteBufferSize(WRITEBUFFERSIZE);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				MyhTables.put(tablename, MyhTable);
			}
		}

		return MyhTables;
	}
	
	
	public void MyBufferFlush(List<HTableInterface> htInterfaces){
		for(HTableInterface htInterface:htInterfaces){
//				htInterface.flushCommits();
		}
	}
	
	public void MyClose(List<HTableInterface> htInterfaces){
		try {
			for(HTableInterface htInterface:htInterfaces){
				htInterface.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String tablename, String[] columnFamilies)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("HTableInterface exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			for (String columnFamily : columnFamilies) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(tableDesc);

			if (admin.tableExists(tablename)) {
				System.out.println("create HTableInterface success!");
			} else {
				System.out.println("create HTableInterface failed!");
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
	
	public Result getRow(HTableInterface HTableInterface, String rowKey) {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = null;
		try {
			result = HTableInterface.get(get);
			result.listCells();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public Map<String, Long> getSizeBatch(String index,List<String> lacciList){
		HTableInterface HTableInterface = HbasePoolOld.getHtable(index);
		byte[] family = Bytes.toBytes("f");
		List<Get> getList = new ArrayList<Get>();
		for(String lacci:lacciList){
			getList.add(new Get(Bytes.toBytes(lacci)).addFamily(family));
		}
		Map<String, Long> SectionResult = new HashMap<String,Long>();
		try {
			Result[] results = HTableInterface.get(getList);
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
		HTableInterface HTableInterface = HbasePoolOld.getHtable(name);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes("f"));
		Result result = null;
		try {
			result = HTableInterface.get(get);
			return (result.listCells().size());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public Map<String, byte[]> getRowBatch(String tableName, Set<String> rowKeys) {
		HTableInterface htable = HbasePoolOld.getHtable(tableName);
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
			ResultMap.put(rowKey.split("\\^")[0],results[i].getValue(HbaseInput.BYTE_f, HbaseInput.BYTE_rts));
			i++;
		}
		return ResultMap;
	}
	
	

	public void delete(String tableName, ArrayList<Delete> deletes) {
		try {
			HTableInterface htable = HbasePoolOld.getHtable(tableName);
			htable.delete(deletes);
			htable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void delete(HTableInterface HTableInterface, String rowKey, String family, String qualifier, String timestamp) {
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

			HTableInterface.delete(delete);
		} catch (Exception e) {

		}
	}

	public ResultScanner getRow_SubstringComparator(HTable HTableInterface, String cf, String column, String Substring)
			throws IOException {
		SubstringComparator comp = new SubstringComparator(Substring);

		SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), column.getBytes(), CompareOp.EQUAL,
				comp);
		Scan scan = new Scan();
		scan.setFilter(filter);

		ResultScanner rs = HTableInterface.getScanner(scan);
		return rs;
	}


}
