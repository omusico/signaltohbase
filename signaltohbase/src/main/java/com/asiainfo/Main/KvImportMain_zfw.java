package com.asiainfo.Main;
import com.ai.util.LogUtil;
import com.asiainfo.HbaseDao.HbaseDao;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class KvImportMain_zfw {
	private static Logger LOG = LogUtil.getLogger(KvImportMain_zfw.class);
	private static int keySize = 0;
	
	public static Map<String, String> SectionInfo = null;
	public static Map<String, Long> SectionResult = null;
	public static List<String> lacci = null;

	Connection conn = null;
	Statement st = null;

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

	public static void exemain(List<String> args) throws SQLException {
		KvImportMain_zfw kv_zfw = new KvImportMain_zfw();
		LOG.info("扫描开始！");
		kv_zfw.getConnection();
		kv_zfw.initSection();
		while (true) {
			long start = System.currentTimeMillis();
			SectionResult = new HashMap();
			try {
				keySize = lacci.size();

				SectionResult = new HbaseDao().getSizeBatch(HbaseDao.TABLE_NAME_INDEX, lacci);

			} catch (Exception e) {
				e.printStackTrace();
				LOG.error(e.getMessage());
			}

			Calendar ca = Calendar.getInstance();
			ca.add(12, -2);
			String minKey = sdf.format(ca.getTime());
			
			Set<String> a = SectionResult.keySet();
			writeToFile(minKey, SectionResult);

			long end = System.currentTimeMillis();
			LOG.info("扫描结束！耗时" + ((end - start) / 1000L) + "s");
			LOG.info("LAC_CI总数为:" + keySize);
			try {
				Thread.sleep(60000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Connection getConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.conn = DriverManager.getConnection("jdbc:mysql://132.151.22.140:3306/app_plc", "root",
					"newroot@!12580");
			return this.conn;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void initSection() throws SQLException {
		SectionInfo = new HashMap();
		lacci = new ArrayList();
		String sql = "select LAC,ci,CITY_ID,COUNTY_ID,LAT,LON from plc_region_section_map where region_type =3";
		ResultSet rs = query(sql);
		LOG.info(sql);
		while (rs.next()) {
			String lac = rs.getString("LAC");
			String ci = rs.getString("CI");
			String CITY_ID = rs.getString("CITY_ID");
			String COUNTY_ID = rs.getString("COUNTY_ID");
			String lat = rs.getString("LAT").substring(0, 9);
			String lon = rs.getString("LON").substring(0, 10);
			SectionInfo.put(lac + "|" + ci, CITY_ID + "," + COUNTY_ID + "," + lat + "," + lon);
			lacci.add(lac + "|" + ci);
		}
		LOG.info(Integer.valueOf(SectionInfo.size()));
		rs.close();
	}

	public ResultSet query(String sql) {
		ResultSet rs = null;
		try {
			if (this.conn != null) {
				Statement st = this.conn.createStatement();

				rs = st.executeQuery(sql);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public static void writeToFile(String dateStr, Map<String, Long> map) {
		BufferedWriter bw = null;
		try {
			if (map.size() == 0) {
				LOG.error("there is no data at this second" + dateStr);
			}
			bw = new BufferedWriter(
					new FileWriter(new File("/home/plczj/workspace/data/IB2005_REDIS/IB2005_" + dateStr)));
			LOG.info("fileName:IB2005_" + dateStr);
			String date = dateStr.substring(0, 8);
			String year = dateStr.substring(0, 4);
			String month = dateStr.substring(4, 6);
			String day = dateStr.substring(6, 8);
			String hour = dateStr.substring(8, 10);
			String minute = dateStr.substring(10, 12);
			String time = year + "-" + month + "-" + day + " " + hour + ":" + minute;
			for (Map.Entry entry : map.entrySet()) {
				String key = (String) entry.getKey();
				long size = ((Long) entry.getValue()).longValue();
				String[] value = ((String) SectionInfo.get(key)).split(",", -1);
				StringBuilder sb = new StringBuilder();
				sb.append(date).append("\t").append(hour).append("\t").append(minute).append("\t").append(time)
						.append("\t").append(value[0]).append("\t").append(value[1]).append("\t").append(value[3])
						.append("\t").append(value[2]).append("\t").append(size);
				bw.write(sb.toString());
				bw.newLine();
			}
			if (map.size() <= 0)
				return;
			bw.close();
			bw = new BufferedWriter(
					new FileWriter(new File("/home/plczj/workspace/data/IB2005_REDIS/IB2005_" + dateStr + ".bdx.chk")));
			bw.write("IB2005_" + dateStr);
		} catch (IOException e) {
			LOG.error("write to file error:" + e.getMessage());
			e.printStackTrace();
		} finally {
			if (bw != null)
				try {
					bw.close();
				} catch (IOException e) {
					LOG.error("close error:" + e.getMessage());
					e.printStackTrace();
				}
		}
	}
}
