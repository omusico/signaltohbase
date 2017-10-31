package com.asiainfo.Main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

import com.asiainfo.HbaseDao.HbaseDao;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.Util.Log4JUtil;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.*;  
import org.apache.hadoop.hbase.client.*;  
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;  

public class HbaseMainNew {
    
	public static void main(String[] args) throws IOException {
		if (args.length == 1 && args[0].equals("test")) {
			System.out.println();
			List<String> lacciList = new ArrayList<String>();
			lacciList.add("55233|1221");
			lacciList.add("55233|1222");
			Map<String, Long> SectionResult = new HbaseDao().getSizeBatch(HbaseDao.TABLE_NAME_INDEX, lacciList);
			for(String key:SectionResult.keySet()){
				System.out.println(key+" "+SectionResult.get(key));
			}
		}
		new HbaseMainNew().doTest();

	}
	
	public void doTest(){
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(System.getProperty("user.dir")+"/test/data.txt")));
			String line = br.readLine();
		
			while(line!=null){
			
				System.out.println(line);
				String mdn = line.substring(0,line.indexOf(","));
				String value = line.substring(line.indexOf(",")+1,line.lastIndexOf(","));
				String roam = line.substring(line.lastIndexOf(",")+1);
				HbaseMain.dataList.add(new String[]{mdn,value,roam});
				if(HbaseMain.dataList.size()>=4){
					new HbaseInput().HbaseSignalPut(HbaseMain.dataList);
				}
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
