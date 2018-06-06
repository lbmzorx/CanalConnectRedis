package FoxdouRedis;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;


public class Task {
	
	public static String filePath="conf/redis/task.json";
	
	public static JSONObject jsonObject;
	public static HashMap<String, Object> task;
	public static ExecutorService cachedThreadPool;
	public static int threadCounter=0;
	
	
	public static void loadConfig() {
		 // 读取原始json文件并进行操作和输出  
        try {  
            //读取属性文件a.properties
		     BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
		     String str = null;
		     StringBuffer sb= new StringBuffer();
		     while((str=bufferedReader.readLine())!=null){
		         sb.append(str);
		     }
		     bufferedReader.close();
          
		     jsonObject = JSONObject.parseObject(sb.toString());
           
		     cachedThreadPool = Executors.newCachedThreadPool();
		     task = new HashMap<String, Object>();
		     
		     for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {		    	 
		    	 TaskConfig taskConfig=new TaskConfig( (JSONObject) entry.getValue() );
		    	 if(taskConfig.taskname==null) {
		    		 taskConfig.taskname=entry.getKey();
		    	 }
		    	 if(taskConfig.checkConfig()) {
		    		 task.put(entry.getKey(),taskConfig);
		    	 }		    	 	    	 
		     }
		     
		   
        } catch (IOException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
        }  
	}
	
	
//	public static void main(String[] args) {  
//		Task.loadConfig();
//	
//		String database="chonglou";
//		String table="yzb_order_sup";
//		String eventType="insert";
//		
//			
//		
//		JSONObject json=new JSONObject();  
//		for (int i=0;i<1;i++) {
//			 json.put("table"+i, JSON.parse("{\"1\":\"aa\"}"));    
//		}
//		Date day=new Date();  
//		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
//		json.put("NOW", JSON.parse("{\"now\":\""+df.format(day).toString()+"\"}"));    
//		
//		
//		for (int i=0;i<10000;i++) {
//			Task.newtask("chonglou","yzb_order_sup","insert",json.toString());
//		}
//		
//
//		cachedThreadPool.shutdown();  
//		System.out.println("End process...");
//    }
	
	
	public static Boolean newtask(String database,String table,String eventType,String jsonstring) {
		System.out.println("Task newtask...");
		for (Entry<String, Object> item : task.entrySet()){
			String key = item.getKey();
			TaskConfig val = (TaskConfig) item.getValue();

			
			if( val.database.equals(database) && val.table.equals(table) && val.eventType.equals(eventType)) {
				if(val.taskDrive.equals("redis")) {	
					cachedThreadPool.execute(new Runnable() {
						public void run() {
							threadCounter++;
							System.out.println("Start task..thread:"+threadCounter);
							if(val.delay>0) {
								try {
									Thread.sleep(val.delay*1000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							
							Boolean result=false;
							if(val.errorRetry>0) {								
								while(val.errorRetry>0 && result==false) {
									result=RedisClient.scriptSet(database,table,eventType,jsonstring,val.taskString);
									val.errorRetry--;
								}
							}else{
								result=RedisClient.scriptSet(database,table,eventType,jsonstring,val.taskString);
							}
							
							if(result==false) {
								TaskConfig.logs(val.errorLog, "ERROR", jsonstring+val.toString());
							}
							System.out.println("end task...thread:"+threadCounter);
							threadCounter--;
						}
					});
					
				}
			}
		}
		return false;
	}
	
	public static Boolean newtask(String database,String table,String eventType,String jsonBefore,String jsonAfter) {
		System.out.println("Task newtask...");
		for (Entry<String, Object> item : task.entrySet()){
			String key = item.getKey();
			TaskConfig val = (TaskConfig) item.getValue();
		    //todo with key and val
		    //WARNING: DO NOT CHANGE key AND val IF YOU WANT TO REMOVE ITEMS LATER
//			System.out.println("Start task...");
			
//			JSONObject json=new JSONObject();			
//	        for (Column column : columns) {    
//	            json.put(column.getName(), column.getValue());    
//	        }
			
			if( val.database.equals(database) && val.table.equals(table) && val.eventType.equals(eventType)) {
				if(val.taskDrive.equals("redis")) {	
					cachedThreadPool.execute(new Runnable() {
						public void run() {
							threadCounter++;
							System.out.println("Start task..thread:"+threadCounter);
							if(val.delay>0) {
								try {
									Thread.sleep(val.delay*1000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							
							Boolean result=false;
							if(val.errorRetry>0) {								
								while(val.errorRetry>0 && result==false) {
									result=RedisClient.scriptSet(database,table,eventType,jsonBefore,jsonAfter,val.taskString);
									val.errorRetry--;
								}
							}else{
								result=RedisClient.scriptSet(database,table,eventType,jsonBefore,jsonAfter,val.taskString);
							}
							
							if(result==false) {
								TaskConfig.logs(val.errorLog, "ERROR", jsonBefore+jsonAfter+val.toString());
							}
							System.out.println("end task...thread:"+threadCounter);
							threadCounter--;
						}
					});
					
				}
			}
		}
		return false;
	}
	
	
}
