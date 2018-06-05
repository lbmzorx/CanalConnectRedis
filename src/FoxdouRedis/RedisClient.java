package FoxdouRedis;

import java.util.List;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;

import redis.clients.jedis.Jedis;  
import redis.clients.jedis.JedisPool;  
import redis.clients.jedis.JedisPoolConfig;  



public class RedisClient {

    protected static JedisPool pool;  

    /** 
     * 
     */  
    static {
    	RedisConfig configFromFile = new RedisConfig();    	
    	configFromFile.loadConfig(); 
    	System.out.println("In task process..."+RedisConfig.host);
    	pool = new JedisPool(RedisConfig.config, RedisConfig.host, RedisConfig.port, 1000);  
    }  

    /** 
     * ��ȡjedisʵ�� 
     */  
    protected static synchronized Jedis getJedis() {  
        Jedis jedis = null;  
        try {  
            jedis = pool.getResource();
            if(RedisConfig.auth!="") {
            	jedis.auth(RedisConfig.auth);
            }            
            jedis.select(RedisConfig.db);
        } catch (Exception e) {  
            e.printStackTrace();  
            if (jedis != null) {  
                pool.returnBrokenResource(jedis);  
            }
            RedisConfig.log("Error", e.getMessage());
        }  
        return jedis;  
    }  

    /** 
     * �ͷ�jedis��Դ 
     *  
     * @param jedis 
     * @param isBroken 
     */  
    protected static void closeResource(Jedis jedis, boolean isBroken) {  
        try {  
            if (isBroken) {  
                pool.returnBrokenResource(jedis);  
            } else {  
                pool.returnResource(jedis);  
            }  
        } catch (Exception e) {  

        }  
    }  

    /** 
     *  �Ƿ����key 
     *  
     * @param key 
     */  
    public static boolean existKey(String key) {  
        Jedis jedis = null;  
        boolean isBroken = false;  
        try {  
            jedis = getJedis();  
            
            return jedis.exists(key);  
        } catch (Exception e) {  
            isBroken = true;  
        } finally {  
            closeResource(jedis, isBroken);  
        }  
        return false;  
    }  

    /** 
     *  ɾ��key 
     *  
     * @param key 
     */  
    public static void delKey(String key) {  
        Jedis jedis = null;  
        boolean isBroken = false;  
        try {  
            jedis = getJedis();  
             
            jedis.del(key);  
        } catch (Exception e) {  
            isBroken = true;  
        } finally {  
            closeResource(jedis, isBroken);  
        }  
    }  

    /** 
     *  ȡ��key��ֵ 
     *  
     * @param key 
     */  
    public static String stringGet(String key) {  
        Jedis jedis = null;  
        boolean isBroken = false;  
        String lastVal = null;  
        try {  
            jedis = getJedis();
            lastVal = jedis.get(key);
            if(RedisConfig.ttl!=-1) {
            	jedis.expire(key, RedisConfig.ttl);  
            }
            
        } catch (Exception e) {  
            isBroken = true;  
        } finally {  
            closeResource(jedis, isBroken);  
        }  
        return lastVal;  
    }  

    /** 
     *  ���string���� 
     *  
     * @param key 
     * @param value 
     */  
    public static String stringSet(String key, String value) {  
        Jedis jedis = null;  
        boolean isBroken = false;  
        String lastVal = null;  
        try {  
            jedis = getJedis();  
              
            lastVal = jedis.set(key, value);  
            if(RedisConfig.ttl!=-1) {
            	jedis.expire(key, RedisConfig.ttl);  
            }
        } catch (Exception e) {  
            e.printStackTrace();  
            isBroken = true;  
        } finally {  
            closeResource(jedis, isBroken);  
        }  
        return lastVal;  
    }  

    /** 
     *  ���hash���� 
     *  
     * @param key 
     * @param field 
     * @param value 
     */  
    public static void hashSet(String key, String field, String value) {  
        boolean isBroken = false;  
        Jedis jedis = null;  
        try {  
            jedis = getJedis();  
            if (jedis != null) {
                jedis.hset(key, field, value);  
                if(RedisConfig.ttl!=-1) {
                	jedis.expire(key, RedisConfig.ttl);  
                }
            }  
        } catch (Exception e) {  
            isBroken = true;  
        } finally {  
            closeResource(jedis, isBroken);  
        }  
    }
   
    
    public static boolean scriptSet(String database,String table,String eventType,String value,String $lua) {
    	 boolean isBroken = false;  
         Jedis jedis = null;  
//         try {  
             jedis = getJedis();  
             if (jedis != null) {
            	 jedis.eval($lua,3,database,table,eventType,value);
             }  
//             return true;
//         } catch (Exception e) {  
//             isBroken = true;
//             RedisConfig.log("error", "redis script error");
//             RedisConfig.log("error", e.getMessage());
//         } finally {  
//             closeResource(jedis, isBroken);
//         }
		return isBroken;  
    }

}
