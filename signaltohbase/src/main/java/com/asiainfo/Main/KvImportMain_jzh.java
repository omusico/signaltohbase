package com.asiainfo.Main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;

import com.asiainfo.HbaseDao.HbaseDao;
import com.asiainfo.HbaseDao.HbaseDaoThread;
import com.asiainfo.Util.Log4JUtil;

public class KvImportMain_jzh {
	private static Logger LOG = Log4JUtil.getLogger(KvImportMain_jzh.class);
	
	BufferedReader br = null;
	BufferedWriter bw = null;

	Set<String> mdn = new HashSet<String>();

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
						long start = System.currentTimeMillis();
						String line;
						String OutPutFileName = getOutPutFileName(file.getName());
						br = new BufferedReader(new FileReader(file));
						bw = new BufferedWriter(new FileWriter(dstFilePath + File.separator + OutPutFileName));
						

						while ((line = br.readLine()) != null) {
							if (line != null && !line.equals("")) {
								mdn.add(deal(line));
							}
						}

						HbaseDaoThread hbaseDaoThread = new HbaseDaoThread();
						Set<String> datas = hbaseDaoThread.HbaseIndexGetter(HbaseDao.TABLE_NAME_INDEX, mdn);
					
						writeToFile(bw, datas);
						long end = System.currentTimeMillis();
						
						LOG.info("查询的LACCI的数量为: "+mdn.size()+" 个, 最终写入文件的MDN的数量为: "+datas.size()+" 个");
						LOG.info("查询文件"+file.getName()+"完成, 耗时"+(end-start)/1000+" 秒");
						LOG.info(dstFilePath+ File.separator +OutPutFileName+"文件写入完毕");
					} catch (FileNotFoundException e) {
						LOG.error(e.getMessage());
						e.printStackTrace();
					} catch (IOException e) {
						LOG.error(e.getMessage());
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
		SimpleDateFormat sdf =new SimpleDateFormat("yyyyMMddHHmmss");
		String ts =sdf.format(new Date());
		int point = fileName.lastIndexOf(".");
		String front = fileName.substring(0,point);
		String behind= fileName.substring(point,fileName.length());
		
		String outputFileName = front+"_"+ts+behind;	
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
	
	public static void exemain(List<String> args) {
		long a = System.currentTimeMillis();
		KvImportMain_jzh kim = new KvImportMain_jzh();

		String inputPath = null;
		String outputPath = null;
		if (args.size() == 0) {
			LOG.info("需要传入输入输出的路径, 输入路径必须, 输出路径可为空");
			LOG.info("如果输出路径为空, 则默认输出到输入路径");
			System.exit(1);
		} else if (args.size()  == 1) {
			inputPath = args.get(0);
			outputPath = args.get(0);
		} else if (args.size()  == 2) {
			inputPath = args.get(0);
			outputPath = args.get(1);
		} else if (args.size()  >= 3) {
			inputPath = args.get(0);
			outputPath = args.get(1);
			HbaseDaoThread.taskSize = Integer.parseInt(args.get(2));
			LOG.info("hbase 并发处理数为: "+HbaseDaoThread.taskSize);
		}
		LOG.info("输入路径为: " + inputPath + ";输出路径为: " + outputPath);
		kim.getValueByFile(inputPath, outputPath);
		long b = System.currentTimeMillis()-a;
		LOG.info("处理共用时 "+b/1000+" 秒"+ "\n________________________________________________________"
				+ "__________________________________________________________"
				+ "_____________________________________________________");
	}

}
