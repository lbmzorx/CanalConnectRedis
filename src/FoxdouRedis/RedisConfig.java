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

public class RedisConfig {
	
	public static String Path="conf/redis/redis.properties";	
	public static String LogFile="logs/connect_redis_logs.log";

	public static String HOST = "127.0.0.1";  
	public static int PORT = 6379;
	public static String AUTH;  
	public static int DB = 0;
	public static int MAX_CONNECT = 1024;  
	public static int MAX_POOL_SPACE = 8;  
	public static int CONNECT_EXPIRE = -1;  
	public static int EXPIRE = 660 * 660 *24;  
	
	
	
	public RedisConfig(){
		 Properties prop = new Properties();   
		 System.out.println("Redis config load begin。。。\\n");
		 try{
			 System.out.println(":in");
		     //读取属性文件a.properties
		     InputStream in = new BufferedInputStream (new FileInputStream(Path));
		     prop.load(in);     ///加载属性列表
		     Iterator<String> it=prop.stringPropertyNames().iterator();
		     while(it.hasNext()){
		         String key=it.next();
		         System.out.println(key+":"+prop.getProperty(key));
		         
		         if(prop.getProperty("redis.host") != null) {
		        	 HOST=prop.getProperty("redis.host");
		         }
		         
		         if(prop.getProperty("redis.auth") != null) {
		        	 AUTH=prop.getProperty("redis.host");
		         }
		         if(prop.getProperty("redis.db") != null) {
		        	 DB = Integer.parseInt(prop.getProperty("redis.db"));
		         }
		         if(prop.getProperty("redis.host") != null) {
		        	 PORT=Integer.parseInt(prop.getProperty("redis.port"));
		         }
		         
		         if(prop.getProperty("redis.host") != null) {
		        	 MAX_CONNECT=Integer.parseInt(prop.getProperty("redis.maxconnect"));
		         }
		         if(prop.getProperty("redis.host") != null) {
		        	 MAX_POOL_SPACE=Integer.parseInt(prop.getProperty("redis.max_pool_space"));
		         }
		         if(prop.getProperty("redis.host") != null) {
		        	 CONNECT_EXPIRE=Integer.parseInt(prop.getProperty("redis.connect_expire"));
		         }
		         if(prop.getProperty("redis.host") != null) {
		        	 EXPIRE=Integer.parseInt(prop.getProperty("redis.expire"));
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
