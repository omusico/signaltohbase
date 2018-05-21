package com.asiainfo.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.*;
import java.util.Properties;

public class TestLog {
	private String basePath = System.getProperty("user.dir");

	public static Long getMutateDel_WriteBufferSize() {
		return MutateDel_WriteBufferSize;
	}

	public static void setMutateDel_WriteBufferSize(Long mutateDel_WriteBufferSize) {
		MutateDel_WriteBufferSize = mutateDel_WriteBufferSize;
	}

	public static Integer getMutatePut_POOL_SIZE() {
		return MutatePut_POOL_SIZE;
	}

	public static void setMutatePut_POOL_SIZE(Integer mutatePut_POOL_SIZE) {
		MutatePut_POOL_SIZE = mutatePut_POOL_SIZE;
	}

	public static String getTABLE_NAME() {
		return TABLE_NAME;
	}

	public static void setTABLE_NAME(String tABLE_NAME) {
		TABLE_NAME = tABLE_NAME;
	}
	
	public static String getKafka_group_id() {
		return kafka_group_id;
	}

	public static void setKafka_group_id(String kafka_group_id) {
		kafka_group_id = kafka_group_id;
	}
	
	public static String kafka_group_id = "signal_hbase_1";

	public static Long    MutateDel_WriteBufferSize = 3145728L;
	public static Integer MutatePut_POOL_SIZE = 1;
	public static String TABLE_NAME = "ZJLT:signal";
	
	
	public static void main(String[] args) throws Exception {

		new TestLog().init();
		System.out.println(MutateDel_WriteBufferSize);
		System.out.println(MutatePut_POOL_SIZE);
		System.out.println(TABLE_NAME);
		System.out.println(kafka_group_id);
	}
	
	public void init(){
		 try {
			File file = new File(basePath+"/conf/config.conf");
			 Properties props=null;
			 InputStream in=null;
			    try
			    {
			        in = new BufferedInputStream (new FileInputStream(file));
			        props = new Properties();
			        props.load(in);
			    }catch(Exception e){
			    	e.printStackTrace();
			    }
				Class<?> clazz = Class.forName("com.asiainfo.Test.TestLog"); 
				Field[] fields = clazz.getFields();
			    for(Field field : fields){
		
		    		if(props.getProperty(field.getName())!=null){
			    		Method mSet = clazz.getDeclaredMethod("set"+field.getName(),field.getType());

		    			if(field.getType().isInstance("java.lang.String")){
		    				mSet.invoke(clazz, props.getProperty(field.getName()));
		    			}else{
				    		Method methodValueOf = field.getType().getMethod("valueOf", String.class);
				    		mSet.invoke(clazz, methodValueOf.invoke(field.getClass(), props.getProperty(field.getName())));
		    			}
		    		}

			    }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
