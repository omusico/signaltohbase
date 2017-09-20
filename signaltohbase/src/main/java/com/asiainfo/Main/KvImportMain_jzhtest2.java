package com.asiainfo.Main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.asiainfo.HbaseDao.HbaseDao;
import com.asiainfo.HbaseDao.HbaseDaoThread;
import com.asiainfo.HbaseDao.HbaseInput;
import com.asiainfo.HbaseDao.HbasePool;

public class KvImportMain_jzhtest2 {

	BufferedReader br = null;
	BufferedWriter bw = null;

	Date currentDate;
	Set<String> sSet = new HashSet<String>();
	Set<String> mdns = new HashSet<String>(); 
	static int lalalalal;

	public void getValueByFile(String sourceFilePath, String dstFilePath) {
		File sourceFile = new File(sourceFilePath);

		if (dstFilePath == null) {
			dstFilePath = sourceFilePath;
		}
		if (sourceFile.isDirectory()) {
			File[] files = sourceFile.listFiles();
			if (files.length > 0) {
				for (File file : files) {
					try {
						br = new BufferedReader(new FileReader(file));
						bw = new BufferedWriter(new FileWriter(dstFilePath + "/" + getOutPutFileName(file.getName())));
						String s;

						currentDate = new Date();

						HTableInterface htInterface = HbasePool.getHtable("ZJLT:signal");
						HTableInterface htInterface_index = HbasePool.getHtable("ZJLT:signalindex");
						while ((s = br.readLine()) != null) {
							if (s != null && !s.equals("")) {
								String mdn = s.split(",",-1)[0];
								Get get = new Get(Bytes.toBytes(mdn));
							}
						}
						
						System.out.println("处理共 "+sSet.size()+" 条lacci");
						HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();
						System.out.println(HbaseDaoThread.taskSize);
						Map<String, Set<String>> datas = hbaseDaoThread.HbaseIndexGetter1(HbaseDao.TABLE_NAME_INDEX, sSet);

						ArrayList<Delete> del = new ArrayList<Delete>();
						
						int allt=0;
						int allf=0;
						for(String a:datas.keySet()){
							String a_lacci = a.split("\\^",-1)[1];
							int t = 0;
							int f = 0;
							HashMap<String,byte[]> signal = hbaseDaoThread.HbaseGetter(HbaseDao.TABLE_NAME, datas.get(a));
							for(String mdn:signal.keySet()){
								if(signal.get(mdn)==null){
								f++;	
								allf++;
								System.out.println(mdn);
								
								Delete delete = new Delete(Bytes.toBytes(a_lacci));
								delete.deleteColumns(HbaseInput.F_byte, Bytes.toBytes(mdn));
								del.add(delete);

								}else{
									String result = new String(signal.get(mdn));
									String[] paras = result.split(",",-1);
									String lacci  = paras[2]+"|"+paras[3];
									if(a_lacci.equals(lacci)){
										t++;
										allt++;
									}else{
										f++;
										allf++;
										System.out.println(mdn);
										
										Delete delete = new Delete(Bytes.toBytes(a_lacci));
										delete.deleteColumns(HbaseInput.F_byte, Bytes.toBytes(mdn));
										del.add(delete);
									}
								}
							}
							System.out.println(a+"   true: "+t+"  false: "+f);
						}
						System.out.println("+++++++++++++++++++++++++++++++++++++++++++++=");
						System.out.println("alltrue: "+allt+"  allfalse: "+allf);
						
						System.out.println("delete");
						new HbaseDaoThread().Hbasedelete(HbaseDao.TABLE_NAME_INDEX,del);
						System.out.println("delete");
//						HbasePool.getHtable("ZJLT:signalindex").delete(del);
						
//						System.out.println("返回共 "+datas.size()+" 条mdn");
//						for(String  onemdn:mdn){
//							mdns.add(onemdn.split("\\^",-1)[1]);
//						}
//						hbaseDaoThread.HbaseGetter(HbaseDao.TABLE_NAME, mdns);
						
						
						
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private String deal(String line) {
		String[] params = line.split("\\^", -1);
		// 每一行的条件至少需要3个字段id^lac^ci
		if (params.length >= 3) {
			// 默认时间片为720分钟
			params[3] = "720";
			if (params.length >= 4) {
				// 如果存在第4个字段，判断下范围是否在1到720之间
				if (Integer.parseInt(params[3]) < 1 || Integer.parseInt(params[3]) > 720) {
					params[3] = "720";
				}
			}
		}
		return new String(new StringBuffer().append(params[0]).append("^")
											.append(params[1]).append("^")
											.append(params[2]).append("^")
											.append(params[3]));
	}

	private String getOutPutFileName(String fileName) {
 		//  MUD.DIM_LAC_CI_YYYYMMDDhhmm.txt
		// TOPIC.XL_USER_Location_201501011201.txt.gz
//		String outputFileName = "TOPIC.XL_USER_Location_" + fileName.substring(15, 27) + ".txt";
		String outputFileName = fileName;
		return outputFileName;
	}

	private void writeToFile(BufferedWriter bw,Set<String> set){
		try {
			for (String txt:set) {
				bw.write(txt);
				bw.newLine();
			}
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void exemain(List<String> args)  {
		long a = System.currentTimeMillis();
		KvImportMain_jzhtest2 kim = new KvImportMain_jzhtest2();
	
		String inputPath = null;
		String outputPath = null;
		if (args.size() == 0) {
			System.out.println("需要传入输入输出的路径，输入路径必须，输出路径可为空");
			System.out.println("如果输出路径为空，则默认输出到输入路径");
			System.exit(1);
		} else if (args.size() == 1) {
			inputPath = args.get(0);
			outputPath = args.get(0);
		} else if (args.size() == 2) {
			inputPath = args.get(0);
			outputPath = args.get(1);
		} else if (args.size() >= 3) {
			inputPath = args.get(0);
			outputPath = args.get(1);
			HbaseDaoThread.taskSize=Integer.parseInt(args.get(2));
		}
		System.out.println("输入路径为：" + inputPath + ";输出路径为：" + outputPath);
		kim.getValueByFile(inputPath, outputPath);
		long b = System.currentTimeMillis()-a;
		System.out.println("处理用时 "+b/1000+" 秒");
	}

}
