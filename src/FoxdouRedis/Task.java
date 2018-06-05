package FoxdouRedis;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;


public class Task {
	
	public static String filePath="conf/redis/task.json";
	
	public static JSONObject jsonObject;
	public static HashMap<String, Object> task;

	
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
	
	
	public static void main(String[] args) {  
		Task.loadConfig();
	
		String database="chonglou";
		String table="yzb_order_sup";
		String eventType="insert";
	
		for (Entry<String, Object> item : task.entrySet()){
			String key = item.getKey();
			TaskConfig val = (TaskConfig) item.getValue();
		    //todo with key and val
		    //WARNING: DO NOT CHANGE key AND val IF YOU WANT TO REMOVE ITEMS LATER
			System.out.println("Start task...");
			
			System.out.println("Start task..."+val.delay);
			if(val.delay>0) {
				try {
					Thread.sleep(val.delay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			JSONObject json=new JSONObject();  
			for (int i=0;i<3;i++) {
				 json.put("table"+i, JSON.parse("{\"1\":\"aa\"}"));    
			}
//	        for (Column column : columns) {    
//	            json.put(column.getName(), column.getValue());    
//	        }    
			System.out.println("Start task..."+val.database+":"+database+(val.database==database));
			if( val.database.equals(database) && val.table.equals(table) && val.eventType.equals(eventType)) {				
				if(val.taskDrive.equals("redis")) {	
					System.out.println("In task process..."+val.taskScript);
					System.out.println("In task process..."+val.taskString);
					RedisClient.scriptSet(database,table,eventType,json.toString(),val.taskScript);
				}
			}
			
		}
		System.out.println("End process...");
    }
	
	
	
	
}
