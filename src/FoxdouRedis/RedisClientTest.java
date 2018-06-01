package FoxdouRedis;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

public class RedisClientTest {
	
	public static String Path="conf/redis/redis.properties";	
	
	
	
	public static void  main(String args[]){
		 Properties prop = new Properties();   
		 System.out.println("adfdasfasdfassafsafasaf");		
		 try{		
		     //redis.properties
		     InputStream in = new BufferedInputStream (new FileInputStream(Path));
		     prop.load(in);     ///���������б�
		     Iterator<String> it=prop.stringPropertyNames().iterator();
		     while(it.hasNext()){
		         String key=it.next();
		         System.out.println(key+":"+prop.getProperty(key));
		     }
		     in.close();

//		     FileOutputStream oFile = new FileOutputStream("b.properties", true);//true��ʾ׷�Ӵ�
//		     prop.setProperty("phone", "10086");
//		     prop.store(oFile, "The New properties file");
//		     oFile.close();
		     System.out.println(":out");
		 }
		 catch(Exception e){
		     System.out.println(e);
		 }	
	}
	
	
}