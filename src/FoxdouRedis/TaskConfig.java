package FoxdouRedis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;

public class TaskConfig {

	public String   taskname;
	public String 	database;
	public String 	table;
	public String 	eventType;
	public String	taskType;		//任务类型
	public String	taskDrive;		//任务驱动
	public String 	taskScript;		//任务 脚本,会将脚本加载为字符串
	public String	taskString;		//任务 字符串，脚本内容加载到这里
	public int 		errorRetry; 	//失败重试次数
	public String 	errorLog  	= "logs/task/";		//失败记录文件
	
	public int 		delay=0;	//滞后执行
	
	private boolean isCheck=false;
	private boolean isError=true;
	
	public TaskConfig(JSONObject jsonConfig) {
		
		if( jsonConfig.getString("taskname") !=null) {
			taskname=jsonConfig.getString("taskname");
		}
		
		if( jsonConfig.getString("database") !=null) {
			database=jsonConfig.getString("database");
		}
		
		if( jsonConfig.getString("table") !=null) {
			table=jsonConfig.getString("table");
		}		
		
		if( jsonConfig.getString("eventType") !=null) {
			eventType=jsonConfig.getString("eventType");
		}
		
		
		if( jsonConfig.getString("taskType") !=null) {
			taskType=jsonConfig.getString("taskType");
		}	
		
		
		if( jsonConfig.getString("taskDrive") !=null) {
			taskDrive=jsonConfig.getString("taskDrive");
		}
		
		
		if( jsonConfig.getInteger("errorRetry") !=null) {
			errorRetry=jsonConfig.getInteger("errorRetry");
		}else {
			errorRetry=1;
		}
		
		Properties properties = System.getProperties();
		
		if( jsonConfig.getString("errorLog") !=null) {
			errorLog=properties.getProperty("user.dir")+"/"+jsonConfig.getString("errorLog");
		}else {
			errorLog=properties.getProperty("user.dir")+"/"+errorLog+taskname+".error.log";
		}

		if( jsonConfig.getInteger("delay") !=null) {
			delay=jsonConfig.getInteger("delay");
		}
		
		if( jsonConfig.getString("taskString") !=null) {
			taskString=jsonConfig.getString("taskString");
		}else {
			if( jsonConfig.getString("taskScript") !=null) {
				taskScript=jsonConfig.getString("taskScript");
				try {  
					 //读取属性文件a.properties
				     BufferedReader bufferedReader = new BufferedReader(new FileReader(properties.getProperty("user.dir")+"/"+taskScript));
				     String str = null;
				     StringBuffer sb= new StringBuffer();
				     while((str=bufferedReader.readLine())!=null){
				         sb.append(str);
				     }
				     bufferedReader.close();
				  
				     taskString=sb.toString();
				 } catch (IOException e) {  
			            // TODO Auto-generated catch block  
			            e.printStackTrace();
			            TaskConfig.logs(errorLog,"error",e.getMessage());
			            this.isCheck=true;
			            this.isError=true;
			     }  
			}
		}		
	}
	
	
	public static Boolean logs(String logfile,String type,String msg) {
		Date day=new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 

		String row="["+df.format(day)+"] "+type+" : "+msg;
		
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(
			new FileOutputStream(logfile, true)));
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
	
	public String toString() {
		return "database:"+database+",table:"+table+",eventType:"+eventType+
				",taskDrive:"+taskDrive+",errorRetry:"+errorRetry+",taskScript:"+taskScript+",taskString:"+taskString;
		
	}
	
	public boolean checkConfig() {		
		
		if( this.taskname ==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}
		
		if( this.database==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}
		
		if( this.table==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}		
		
		if( this.eventType==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}		
		
		if( this.taskType==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}		
		
		if( this.taskDrive==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}
		
		if( this.taskString==null) {
			this.isError=true;
			this.isCheck=true;
			return false;
		}
		this.isError=false;
		this.isCheck=false;
		return true;
	}

	public boolean isError() {
		if (this.isCheck && this.isError) {
			return true;
		}else {
			return false;
		}
	}
}
