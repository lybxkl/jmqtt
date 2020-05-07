package org.jmqtt.broker.service.impl;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jmqtt.broker.service.LogProducerService;
import org.jmqtt.common.bean.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: IoT-Plat
 * @description:  kafka生产者
 * @author: Mr.Liu
 * @create: 2020-04-14 09:26
 **/
public class LogProducerServiceImpl implements LogProducerService {

    private int nTreads = 10;
    private String topic = "MQ_MSG";
    private int defaultTreads = 10;
    private int maxTreads = 100;
    private static final Logger logger = LoggerFactory.getLogger(LogProducerServiceImpl.class);

    private ExecutorService executorService = Executors.newFixedThreadPool(nTreads<=0?defaultTreads:Math.min(nTreads,maxTreads));

    private static KafkaProducer<String, byte[]> producer;

    public LogProducerServiceImpl() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.112.26.129:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory",33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        producer = new KafkaProducer<String, byte[]>(props);
    }

    @Override
    public void sendMsg(Message log) {
        executorService.execute(() -> {
            try {
                ProducerRecord<String, byte[]> record =  new ProducerRecord<String,byte[]>(topic,
                        0,"MQ_MSG",log.toString().getBytes());
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e==null){
                            logger.error("发送MQTT消息到Kafka成功!--->topic：{}，offset：{},partition：{}", recordMetadata.topic(),recordMetadata.offset(),recordMetadata.partition());
                        }else {
                            logger.error("添加MQTT消息到kafka失败!--->topic：{}，offset：{}，,partition：{}---》error:{}", recordMetadata.topic(),recordMetadata.offset(),recordMetadata.partition(), e.getMessage());
                        }

                    }
                });
            } catch (Exception e) {
                logger.error("记录普通日志到kafka错误!---msg：{}，error：{}",log.toString(), e.getMessage());
            }
        });
    }
}
