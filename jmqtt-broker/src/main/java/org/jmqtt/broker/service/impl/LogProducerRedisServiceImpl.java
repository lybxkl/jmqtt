package org.jmqtt.broker.service.impl;

import com.alibaba.fastjson.JSON;
import org.jmqtt.broker.service.LogProducerService;
import org.jmqtt.broker.utils.RedisPool;
import org.jmqtt.common.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @program: jmqtt
 * @description: redis版本
 * @author: Mr.Liu
 * @create: 2020-04-29 20:22
 **/
public class LogProducerRedisServiceImpl implements LogProducerService {
    private static final Logger logger = LoggerFactory.getLogger(LogProducerRedisServiceImpl.class);
    private volatile static Jedis jedis;
    private Jedis getJedis(){
        if(jedis==null){
            synchronized (Jedis.class){
                if(jedis==null){
                    jedis= RedisPool.getJedis();
                }
            }
        }
        return jedis;
    }
    public LogProducerRedisServiceImpl() {
        /**通过定时任务每隔3秒将mq消息发送到redis中去，这个是在这一次执行完之后才等待三秒继续**/
        executor = Executors.newScheduledThreadPool(2);
        executor.scheduleWithFixedDelay(()->{
            open01.set(!open01.get());
            if (!open01.get()){
                if(mqList01.size()>0){
                    try {
                        Long ls = getJedis().rpush(redisListKey, mqList01.toArray(new String[0]));
                        logger.info("redis中存在{}条消息待处理: ",ls);
                        mqList01 = new ArrayList<>(size);
                    }catch (Exception e){
                        logger.error("持久化mq消息到redis错误，当前集合是mqList01",e);
                    }
                }
            }else {
                if(mqList02.size()>0){
                    try {
                        Long ls = getJedis().rpush(redisListKey, mqList02.toArray(new String[0]));
                        mqList02 = new ArrayList<>(size);
                        logger.info("redis中存在{}条消息待处理: ",ls);
                    }catch (Exception e){
                        logger.error("持久化mq消息到redis错误，当前集合是mqList02",e);
                    }
                }
            }
        }, 2, 3, TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(()->{
            //TODO 这里没有直接使用pop,这个总感觉不好，要是出错了，岂不是丢数据了
            // 采用先lrange(),然后再ltrim()，
            // 这个ltrim(key,start,-1)使用要特别注意，他和lrange()有所不同
            // 它是删除索引start到最右端之外的所有元素，注意删除的元素不包含索引start的元素
            try {
                List<String> mq = getJedis().lrange(redisListKey,0,-1);
                if(Objects.nonNull(mq) && mq.size()>0){
                    mq.forEach(v->{
                        Message m =  JSON.parseObject(v, Message.class);
                    });
                    // TODO 保存到数据库里面去，注意添加事务

                    logger.info("从redis消费{}条mq消息：",mq.size());
                    getJedis().ltrim(redisListKey,mq.size(),-1);
                }
            }catch (Exception e){
                logger.error("消费redis中mq数据错误",e);
            }
        }, 1, 3, TimeUnit.SECONDS);
    }

    /**redis 中list的key**/
    private static final String redisListKey = "MQ_LIST";
    /**集合初始大小**/
    private static Integer size = 100;
    /**两个集合切换保存mq数据**/
    private static List<String> mqList01 = new ArrayList<>(size);
    private static List<String> mqList02 = new ArrayList<>(size);
    /**表示采用那个集合来保存数据**/
    private volatile static AtomicBoolean open01 = new AtomicBoolean(true);
    private static ScheduledExecutorService executor = null;


    /**
     * 通过redis list结果存储mq数据，等待消费者来消费
     * @param log
     */
    @Override
    public void sendMsg(Message log) {
        if(open01.get()){
            mqList01.add(JSON.toJSONString(log));
        }else {
            mqList02.add(JSON.toJSONString(log));
        }
    }
}
