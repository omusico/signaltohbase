package com.asiainfo.Main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.asiainfo.HbaseDao.HbaseDao;
import com.asiainfo.HbaseDao.HbaseDaoThread;
import com.asiainfo.Util.Log4JUtil;
	
public class KvImportMain_sfz {
	private static Logger LOG = Log4JUtil.getLogger(KvImportMain_sfz.class);
	
	BufferedReader br = null;
	BufferedWriter bw = null;

	public void getValueByFile(String sourceFilePath, String dstFilePath) {
		File sourceFile = new File(sourceFilePath);

		if (dstFilePath == null) {
			dstFilePath = sourceFilePath;
		}
		if (sourceFile.isDirectory()) {
			File[] files = sourceFile.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if((pathname.getName().startsWith("MUD.DIM_LAC_CI")) && (pathname.getName().endsWith("txt"))){
						return true;
					}
					return false;
				}
			});
			if (files.length > 0) {
				for (File file : files) {
		            long start = System.currentTimeMillis();
					String OutPutFileName = getOutPutFileName(file.getName());
					Set<String> id_lacciSet = new HashSet<String>();
					Set<String> resultSet = new HashSet<String>();
					try {
						br = new BufferedReader(new FileReader(file));
						bw = new BufferedWriter(new FileWriter(dstFilePath + File.separator + OutPutFileName));
						String line;


						while ((line = br.readLine()) != null) {
							if (line != null && !line.equals("")) {
								id_lacciSet.add(deal(line));
							}
						}

						HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();		
						Map<String, Set<String>> datas = hbaseDaoThread.HbaseIndexGetter1(HbaseDao.TABLE_NAME_INDEX, id_lacciSet);
						
						for(String data:datas.keySet()){
							String data_id = data.split("\\^",-1)[0];
							String data_lacci = data.split("\\^",-1)[1];
							HashMap<String,byte[]> signal = hbaseDaoThread.HbaseGetter(HbaseDao.TABLE_NAME, datas.get(data));
							
							for(String mdn:signal.keySet()){
								if(!(signal.get(mdn)==null)){
									String result = new String(signal.get(mdn));
									String[] paras = result.split(",",-1);
									String lacci  = paras[2]+"|"+paras[3];
									if(data_lacci.equals(lacci)){
										resultSet.add(new String(
												new StringBuffer()
												.append(data_id).append("^")
												.append(mdn).append("^")
												.append(paras[0]).append("^")
												.append(getlacLocShi(paras[1])).append("^")
												.append(paras[1])
												));
									}
								}
							}
						}
						writeToFile(bw, resultSet);
						
						long end = System.currentTimeMillis();
						LOG.info("查询的LACCI的数量为: "+id_lacciSet.size()+" 个, 最终写入文件的MDN的数量为: "+resultSet.size()+" 个");
						LOG.info("查询文件"+file.getName()+"完成, 耗时"+(end-start)/1000+" 秒");
						LOG.info(dstFilePath+ File.separator +OutPutFileName+"文件写入完毕");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					file.delete();
				}
			}
		}
	}

	private String deal(String line) {
		String[] params = line.split("\\^", -1);
		// 每一行的条件至少需要3个字段id^lac^ci
		if (params.length == 3) {
			// 默认时间片为720分钟
			params[3] = "720";
		}
		if (params.length >= 4) {
			// 如果存在第4个字段，判断下范围是否在1到720之间
			if(!params[3].equals("")){
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
		String outputFileName = "TOPIC.XL_USER_Location_" + fileName.substring(15, 27) + ".txt";
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
	
	private String getlacLocShi(String lacLocXian){
		if((("").equals(lacLocXian)))
			return "";
		else{
			if(lacLocXian.startsWith("12101"))
				return "571";
			if(lacLocXian.startsWith("12102"))
				return "574";
			if(lacLocXian.startsWith("12103"))
				return "577";
			if(lacLocXian.startsWith("12104"))
				return "573";
			if(lacLocXian.startsWith("12105"))
				return "572";
			if(lacLocXian.startsWith("12106"))
				return "575";
			if(lacLocXian.startsWith("12107"))
				return "579";
			if(lacLocXian.startsWith("12108"))
				return "570";
			if(lacLocXian.startsWith("12109"))
				return "580";
			if(lacLocXian.startsWith("12110"))
				return "576";
			if(lacLocXian.equals("12111"))
				return "578";
			else
			    return "";
		}
	}
	
	public static void exemain(List<String> args) {
		long a = System.currentTimeMillis();
		KvImportMain_sfz kim = new KvImportMain_sfz();

		String inputPath = null;
		String outputPath = null;
		if (args.size() == 0) {
			LOG.info("需要传入输入输出的路径, 输入路径必须, 输出路径可为空");
			LOG.info("如果输出路径为空, 则默认输出到输入路径");
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
			HbaseDaoThread.taskSize = Integer.parseInt(args.get(2));
			LOG.info("hbase 并发处理数为: "+HbaseDaoThread.taskSize);
		}
		LOG.info("输入路径为: " + inputPath + ";输出路径为: " + outputPath);
		kim.getValueByFile(inputPath, outputPath);
		long b = System.currentTimeMillis()-a;
		LOG.info("处理共用时 "+b+" 秒"+ "\n________________________________________________________"
				+ "__________________________________________________________"
				+ "_____________________________________________________");
	}

}
