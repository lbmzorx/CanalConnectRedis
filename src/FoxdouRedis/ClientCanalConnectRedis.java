package FoxdouRedis;


import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.fastjson.JSONObject;

public class ClientCanalConnectRedis {

	public static void main(String args[]) {
	    CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(), 11111), "example", "", "");
	    int batchSize = 1000;
	    int emptyCount = 0;
	    try {
	        connector.connect();
	        connector.subscribe(".*\\..*");
	        connector.rollback();
	        
	        Task.loadConfig();
	        
	        int totalEmptyCount = 120;
	        while (emptyCount < totalEmptyCount) {
	            Message message = connector.getWithoutAck(batchSize); // ��ȡָ������������
	            long batchId = message.getId();
	            int size = message.getEntries().size();
	            if (batchId == -1 || size == 0) {
	                emptyCount++;
	                System.out.println("empty count : " + emptyCount);
	                try {
	                    Thread.sleep(1000);
	                } catch (InterruptedException e) {
	                }
	            } else {
	                emptyCount = 0;
	                // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
	                printEntry(message.getEntries());
	            }
	
	            connector.ack(batchId); // �ύȷ��
	            // connector.rollback(batchId); // ����ʧ��, �ع�����
	        }
	
	        System.out.println("empty too many times, exit");
	    } finally {
	        connector.disconnect();
	    }
	}

	private static void printEntry(List<Entry> entrys) {
	    for (Entry entry : entrys) {
	        if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
	            continue;
	        }
	
	        RowChange rowChage = null;
	        try {
	            rowChage = RowChange.parseFrom(entry.getStoreValue());
	        } catch (Exception e) {
	            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
	                                       e);
	        }
	
	        EventType eventType = rowChage.getEventType();
	        System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
	                                         entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
	                                         entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
	                                         eventType));
	       
	        for (RowData rowData : rowChage.getRowDatasList()) {
	        	if (eventType == EventType.DELETE) {
	        		redisDelete(entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),rowData.getBeforeColumnsList());
                   
                } else if (eventType == EventType.INSERT) {
                    redisInsert(entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),rowData.getAfterColumnsList());
                } else {  
                    System.out.println("-------> before");  
                    printColumn(rowData.getBeforeColumnsList());  
                    System.out.println("-------> after");  
                    printColumn(rowData.getAfterColumnsList());  
                    redisUpdate(entry.getHeader().getSchemaName(),entry.getHeader().getTableName(),rowData.getBeforeColumnsList(),rowData.getAfterColumnsList());
                }  
	        	
	        	 printColumn(rowData.getBeforeColumnsList());  
	        	System.out.println("ttt task..."+rowData.toString());
	        
	        }
	    }
	   
	}

	private static void printColumn(List<Column> columns) {
	    for (Column column : columns) {
	        System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
	    }
	}
	
	
	private static void redisInsert(String database,String table, List<Column> columns){  
        JSONObject json=new JSONObject();  
        for (Column column : columns) {    
            json.put(column.getName(), column.getValue());    
         }    
        if(columns.size()>0){ 
            Task.newtask(database,table,"INSERT",json.toString());
        }  
     }  

    private static void redisUpdate(String database,String table, List<Column> columnsBefore,List<Column> columnsAfter){  
        JSONObject jsonBefore=new JSONObject();  
        JSONObject jsonAfter=new JSONObject();  
        for (Column column : columnsBefore) {    
        	jsonBefore.put(column.getName(), column.getValue());    
        }  
        
        for (Column column : columnsBefore) {    
        	jsonAfter.put(column.getName(), column.getValue());    
        }  
        if(columnsAfter.size()>0){
        	Task.newtask(database,table,"UPDATE",jsonBefore.toString(),jsonAfter.toString());
        }  
    }  

     private static  void redisDelete(String database,String table, List<Column> columns){  
         JSONObject json=new JSONObject();  
	        for (Column column : columns) {    
	            json.put(column.getName(), column.getValue());    
	         }    
	        if(columns.size()>0) {
	            Task.newtask(database,table,"DELETE",json.toString());
	        }  
     }  
	
}
