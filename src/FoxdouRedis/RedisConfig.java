package FoxdouRedis;


import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPoolConfig;

public class RedisConfig {
	
	public static String Path="conf/redis/redis.properties";	
	public static String LogFile="logs/redis/connect_redis_logs.log";


	
	public static String 	host="127.0.0.1";
	public static int 		port=6379;
	public static String 	auth;
	public static int 		db=1;
	public static int 		ttl;


	public static int 		MaxIdle=30;
	public static int 		MaxTotal=1024;
	public static int 		MaxWaitMillis=-1;
	public static int 		MinEvictableIdleTimeMillis=1800000;
	public static int 		MinIdle=0;
	
	public static int 		NumTestsPerEvictionRun;
	public static int 		SoftMinEvictableIdleTimeMillis;
	public static Boolean 	TestOnBorrow;
	public static Boolean 	TestWhileIdle;
	public static int 		TimeBetweenEvictionRunsMillis;
	public static Boolean 	BlockWhenExhausted;
	public static String 	EvictionPolicyClassName;
	public static Boolean	JmxEnabled;
	public static String 	JmxNamePrefix;
	public static Boolean 	Lifo;
	public static JedisPoolConfig config;

	
	public void loadConfig(){
		 Properties prop = new Properties();
		 System.out.println("Redis config load begin。。。\\n");
		 config = new JedisPoolConfig();
		
		 
		 try{
			 System.out.println(":in");
		     //读取属性文件a.properties
		     InputStream in = new BufferedInputStream (new FileInputStream(Path));
		     prop.load(in);     ///加载属性列表
		     Iterator<String> it=prop.stringPropertyNames().iterator();
		     while(it.hasNext()){
		         String key=it.next();
		         System.out.println(key+":"+prop.getProperty(key));
		         
		         if(prop.getProperty("host") != null) {
		        	 	host=prop.getProperty("host");
		        	}
		        	if(prop.getProperty("port") != null) {
		        	 	port=Integer.parseInt(prop.getProperty("port"));
		        	}
		        	if(prop.getProperty("auth") != null) {
		        	 	auth=prop.getProperty("auth");
		        	}
		        	if(prop.getProperty("db") != null) {
		        	 	db=Integer.parseInt(prop.getProperty("db"));
		        	}
		        	if(prop.getProperty("db") != null) {
		        		ttl=Integer.parseInt(prop.getProperty("ttl"));
		        	}
		        	
		        	if(prop.getProperty("MaxIdle") != null) {
		        	 	MaxIdle=Integer.parseInt(prop.getProperty("MaxIdle"));
		        	 	 config.setMaxIdle(MaxIdle);
		        	}
		        	if(prop.getProperty("MaxTotal") != null) {
		        	 	MaxTotal=Integer.parseInt(prop.getProperty("MaxTotal"));
		        	 	 config.setMaxTotal(MaxTotal);
		        	}
		        	if(prop.getProperty("MaxWaitMillis") != null) {
		        	 	MaxWaitMillis=Integer.parseInt(prop.getProperty("MaxWaitMillis"));
		        	 	config.setMaxWaitMillis(MaxWaitMillis);
		        	}
		        	if(prop.getProperty("MinEvictableIdleTimeMillis") != null) {
		        	 	MinEvictableIdleTimeMillis=Integer.parseInt(prop.getProperty("MinEvictableIdleTimeMillis"));
		        	 	config.setMinEvictableIdleTimeMillis(MinEvictableIdleTimeMillis);
		        	}
		        	if(prop.getProperty("MinIdle") != null) {
		        	 	MinIdle=Integer.parseInt(prop.getProperty("MinIdle"));
		        	 	config.setMinIdle(MinIdle);
		        	}
		        	
		        	if(prop.getProperty("NumTestsPerEvictionRun") != null) {
		        	 	NumTestsPerEvictionRun=Integer.parseInt(prop.getProperty("NumTestsPerEvictionRun"));
		        	 	config.setNumTestsPerEvictionRun(NumTestsPerEvictionRun);
		        	}
		        	if(prop.getProperty("SoftMinEvictableIdleTimeMillis") != null) {
		        	 	SoftMinEvictableIdleTimeMillis=Integer.parseInt(prop.getProperty("SoftMinEvictableIdleTimeMillis"));
		        	 	config.setSoftMinEvictableIdleTimeMillis(SoftMinEvictableIdleTimeMillis);
		        	}
		        	if(prop.getProperty("TestOnBorrow") != null) {
		        	 	TestOnBorrow=Boolean.parseBoolean(prop.getProperty("TestOnBorrow"));
		        	 	config.setTestOnBorrow(TestOnBorrow);
		        	}
		        	if(prop.getProperty("TestWhileIdle") != null) {
		        	 	TestWhileIdle=Boolean.parseBoolean(prop.getProperty("TestWhileIdle"));
		        	 	config.setTestWhileIdle(TestWhileIdle);
		        	}
		        	if(prop.getProperty("TimeBetweenEvictionRunsMillis") != null) {
		        	 	TimeBetweenEvictionRunsMillis=Integer.parseInt(prop.getProperty("TimeBetweenEvictionRunsMillis"));
		        	 	config.setTimeBetweenEvictionRunsMillis(TimeBetweenEvictionRunsMillis);
		        	}
		        	if(prop.getProperty("BlockWhenExhausted") != null) {
		        	 	BlockWhenExhausted=Boolean.parseBoolean(prop.getProperty("BlockWhenExhausted"));
		        	 	config.setBlockWhenExhausted(BlockWhenExhausted);
		        	}
		        	if(prop.getProperty("EvictionPolicyClassName") != null) {
		        	 	EvictionPolicyClassName=prop.getProperty("EvictionPolicyClassName");
		        	 	config.setEvictionPolicyClassName(EvictionPolicyClassName);
		        	}
		        	if(prop.getProperty("JmxEnabled") != null) {
		        	 	JmxEnabled=Boolean.parseBoolean(prop.getProperty("JmxEnabled"));
		        	 	config.setJmxEnabled(JmxEnabled);
		        	}
		        	if(prop.getProperty("JmxNamePrefix") != null) {
		        	 	JmxNamePrefix=prop.getProperty("JmxNamePrefix");
		        	 	config.setJmxNamePrefix(JmxNamePrefix);
		        	}
		        	if(prop.getProperty("Lifo") != null) {
		        	 	Lifo=Boolean.parseBoolean(prop.getProperty("Lifo"));
		        	 	config.setLifo(Lifo);
		        	}
		         
		     }
		     in.close();
		     System.out.println("Redis config load end。。。\n");
		 }
		 catch(Exception e){
		     System.out.println(e);		     
		     RedisConfig.log("Error",e.getMessage());
		 }
	
	}
	
	/**
	 * 
	 * @param type
	 * @param msg
	 * @return
	 */
	public static boolean log(String type,String msg) {		
		Date day=new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 

		String row="["+df.format(day)+"] "+type+" : "+msg;
		
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(
			new FileOutputStream(LogFile, true)));
			out.write(row+"\r\n");
			out.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
		return false;
	}

	
	
}
