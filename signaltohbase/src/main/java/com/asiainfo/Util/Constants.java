package com.asiainfo.Util;

public class Constants {
	/**
	 * 位置信令
	 */
	public static final int LOCSIGNAL = 0;
	/**
	 * 语音信令
	 */
	public static final int VOCSIGNAL = 1;
	/**
	 * 短信信令
	 */
	public static final int SMSSIGNAL = 2;
	/**
	 * 插入REDIS库数据过期时间为12小时
	 */
	public static final int REALTIME_TIMEOUT = 12 * 3600;
	/**
	 * 是否丢弃过期数据
	 */
	public static boolean ABANDON_EXPIRE_DATA = false;
}
