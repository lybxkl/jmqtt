package org.jmqtt.broker.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ResourceBundle;

/**
 * @program: jmqtt
 * @description: redis连接池
 * @author: Mr.Liu
 * @create: 2020-05-05 21:00
 **/
@Component
public class RedisPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisPool.class);
    private static JedisPool jedisPool;
    private static int maxtotal;
    private static int maxwaitmillis;
    private static String host;
    private static int port;
    private static int db;
    private static String password;

    /*读取jedis.properties配置文件*/
    static{
        ResourceBundle rb = ResourceBundle.getBundle("redis");
        maxtotal = Integer.parseInt(rb.getString("maxtotal"));
        maxwaitmillis = Integer.parseInt(rb.getString("maxwaitmillis"));
        host = rb.getString("host");
        port = Integer.parseInt(rb.getString("port"));
        db = Integer.parseInt(rb.getString("db"));
        password = rb.getString("password");
        logger.info("RedisPool config ：MAXTOTAL:{}，MAXWAITMILLIS:{}，HOST:{}，PORT:{}，db:{}，PASSWORD:{}",maxtotal,maxwaitmillis,host,port,db,password);
    }

    /*创建连接池*/
    static{
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(maxtotal);
        jedisPoolConfig.setMaxWaitMillis(maxwaitmillis);
        jedisPool = new JedisPool(jedisPoolConfig,host,port);
        //jedisPool.getResource().set("key","key");
        logger.info("jedisPool init success");
    }
    public static Jedis getJedis(){
        Jedis jedis = jedisPool.getResource();
        //jedis.auth(password);
        jedis.select(db);
        return jedis;
    }
    public static Jedis getJedis(int dbs){
        Jedis jedis = jedisPool.getResource();
        jedis.auth(password);
        jedis.select(dbs);
        return jedis;
    }
}
