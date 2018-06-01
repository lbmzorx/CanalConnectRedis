package FoxdouRedis;

import redis.clients.jedis.Jedis;  
import redis.clients.jedis.JedisPool;  
import redis.clients.jedis.JedisPoolConfig;  



public class RedisClient {

//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		System.out.println("Hello World!!!");
//		
//		  Jedis jedis = new Jedis("127.0.0.1", 6379);
//	        jedis.auth("sl788zdfwc8zi");
//		  	jedis.select(0);
//
//	        jedis.set("redis_first", "hello");
//	        System.out.println("key redis_first:"+jedis.get("redis_first"));
//		
//	}
	
	 //
    private static String ADDR = "127.0.0.1";  

    //
    private static int PORT = 6379;

    // 
    private static String AUTH = "sl788zdfwc8zi";  

    private static int db = 7;
    
    // 
    // 
    private static int MAX_ACTIVE = 1024;  

    // 
    private static int MAX_IDLE = 200;  

    // 
    private static int MAX_WAIT = 10000;  

    //
    protected static int  expireTime = 660 * 660 *24;  

    //
    protected static JedisPool pool;  

    /** 
     * 
     */  
    static {
    	RedisConfig configFromFile = new RedisConfig();
    	
        JedisPoolConfig config = new JedisPoolConfig();  
        
        
        // 
        config.setMaxTotal(MAX_ACTIVE);  
        //
        config.setMaxIdle(MAX_IDLE);  
        //
        config.setMaxWaitMillis(MAX_WAIT);  
        
        config.setTestOnBorrow(false);  
        pool = new JedisPool(config, ADDR, PORT, 1000);  
    }  

    /** 
     * ��ȡjedisʵ�� 
     */  
    protected static synchronized Jedis getJedis() {  
        Jedis jedis = null;  
        try {  
            jedis = pool.getResource();
            jedis.auth(RedisClient.AUTH);
            jedis.select(RedisClient.db);
        } catch (Exception e) {  
            e.printStackTrace();  
            if (jedis != null) {  
                pool.returnBrokenResource(jedis);  
            }  
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
            jedis.select(RedisClient.db);  
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
            jedis.expire(key, expireTime);  
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
            jedis.expire(key, expireTime);  
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
                jedis.expire(key, expireTime);  
            }  
        } catch (Exception e) {  
            isBroken = true;  
        } finally {  
            closeResource(jedis, isBroken);  
        }  
    }  
	
    
    public static void scriptSet() {
    	
    }

}
