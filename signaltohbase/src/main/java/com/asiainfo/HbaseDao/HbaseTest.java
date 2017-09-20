package com.asiainfo.HbaseDao;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {

	public static void main(String[] args){
		if(args[0].equals("aaa")){
			Set<String> mdn = new HashSet<String>();
			mdn.add("13855|29005");
			HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();
			hbaseDaoThread.taskSize = 1;
			Set<String> datas = hbaseDaoThread.HbaseIndexGetter("ZJLT:signalindex", mdn);
			for(String data:datas){
				System.out.println(data);
			}
		}else if(args[0].equals("bbb")){
			try {	
				HTableInterface htInterface = HbasePool.getHtable("ZJLT:signalindex");
				                                                   
				Get get = new Get(Bytes.toBytes("13855|29005"));
				get.addFamily(Bytes.toBytes("f"));
				long current = System.currentTimeMillis();
				
				if(args.length>1){
					get.setTimeRange(current-(Long.parseLong(args[1])*1000), current);
					System.out.println(current-(Long.parseLong(args[1])*1000));
					System.out.println(current);
				}

				
				Result result = htInterface.get(get);
				CellScanner cs = result.cellScanner();
				
				while(cs.advance()){
					Cell cell = cs.current();
					System.out.println(new String(CellUtil.cloneRow(cell)));
					String qualifier = new String(CellUtil.cloneQualifier(cell));
					System.out.println(qualifier);
				}
				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else if(args[0].equals("ccc")){
			HbaseDao hbaseDao = new HbaseDao();
			HbaseInput hbaseInput = new HbaseInput();
			HTableInterface htInterface = HbasePool.getHtable("ZJLT:signalindex");
			hbaseDao.putRow(htInterface, "13855|29005", "f", args[1], "", Long.parseLong(args[2]));	
		}
	}
}
