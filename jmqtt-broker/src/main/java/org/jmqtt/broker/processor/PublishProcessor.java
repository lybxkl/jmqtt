package org.jmqtt.broker.processor;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.jmqtt.broker.BrokerController;
import org.jmqtt.broker.acl.PubSubPermission;
import org.jmqtt.broker.service.LogProducerService;
import org.jmqtt.broker.service.impl.LogProducerRedisServiceImpl;
import org.jmqtt.broker.utils.RedisPool;
import org.jmqtt.store.FlowMessageStore;
import org.jmqtt.remoting.session.ClientSession;
import org.jmqtt.common.bean.Message;
import org.jmqtt.common.bean.MessageHeader;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.netty.RequestProcessor;
import org.jmqtt.remoting.session.ConnectManager;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.remoting.util.NettyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class PublishProcessor extends AbstractMessageProcessor implements RequestProcessor {
    private Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);
    private String AppHeard = "APP_";
    private String DevHeard = "DEV_";
    private volatile static Jedis jedis;
    private Jedis getJedis(){
        if(jedis==null){
            synchronized (Jedis.class){
                if(jedis==null){
                    jedis= RedisPool.getJedis(3);
                }
            }
        }
        return jedis;
    }
    private volatile static LogProducerService service;
    public static LogProducerService getProService(){
        if(service==null){
            synchronized (LogProducerService.class){
                if(service==null){
                    //service = new LogProducerServiceImpl();
                    service = new LogProducerRedisServiceImpl();
                }
            }
        }
        return service;
    }
    private FlowMessageStore flowMessageStore;

    private PubSubPermission pubSubPermission;

    public PublishProcessor(BrokerController controller){
        super(controller.getMessageDispatcher(),controller.getRetainMessageStore(),controller.getInnerMessageTransfer());
        this.flowMessageStore = controller.getFlowMessageStore();
        this.pubSubPermission = controller.getPubSubPermission();
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        try{
            MqttPublishMessage publishMessage = (MqttPublishMessage) mqttMessage;
            MqttQoS qos = publishMessage.fixedHeader().qosLevel();
            Message innerMsg = new Message();
            String clientId = NettyUtil.getClientId(ctx.channel());
            ClientSession clientSession = ConnectManager.getInstance().getClient(clientId);
            String topic = publishMessage.variableHeader().topicName();
            if(!this.pubSubPermission.publishVerfy(clientId,topic)){
                log.warn("[PubMessage] permission is not allowed");
                clientSession.getCtx().close();
                return;
            }
            //下面是专门处理app上传的控制数据的过滤
            if(clientId.substring(0,AppHeard.length()).equals(AppHeard)){
                //校验是否有资格发出控制指令数据
                String[] b = ((MqttPublishMessage) mqttMessage).payload().toString(CharsetUtil.UTF_8).split("_");
                if(b.length<2){
                    log.warn("app上传控制指令数据格式错误");
                    writeError(ctx,"该控制指令数据格式错误");
                    return;
                }
                if(1!=fromJson(getJedis().get(AppHeard+b[0]),Integer.class)){
                    log.warn("未授权该指令");
                    writeError(ctx,"该指令未授权");
                    return;
                }
                byte[] by = b[1].getBytes(CharsetUtil.UTF_8);
                innerMsg.setPayload(by);
            }else {
                innerMsg.setPayload(MessageUtil.readBytesFromByteBuf(((MqttPublishMessage) mqttMessage).payload()));
            }
            innerMsg.setClientId(clientId);
            innerMsg.setType(Message.Type.valueOf(mqttMessage.fixedHeader().messageType().value()));
            Map<String,Object> headers = new HashMap<>();
            headers.put(MessageHeader.TOPIC,publishMessage.variableHeader().topicName());
            headers.put(MessageHeader.QOS,publishMessage.fixedHeader().qosLevel().value());
            headers.put(MessageHeader.RETAIN,publishMessage.fixedHeader().isRetain());
            headers.put(MessageHeader.DUP,publishMessage.fixedHeader().isDup());
            innerMsg.setHeaders(headers);
            innerMsg.setMsgId(publishMessage.variableHeader().packetId());

            //System.err.println(innerMsg.toString());
            /**保存信息**/
            getProService().sendMsg(innerMsg);

            switch (qos){
                case AT_MOST_ONCE:
                    processMessage(innerMsg);
                    break;
                case AT_LEAST_ONCE:
                    processQos1(ctx,innerMsg);
                    break;
                case EXACTLY_ONCE:
                    processQos2(ctx,innerMsg);
                    break;
                default:
                    log.warn("[PubMessage] -> Wrong mqtt message,clientId={}", clientId);
            }
        }catch (Throwable tr){
            log.warn("[PubMessage] -> Solve mqtt pub message exception:{}",tr);
        }finally {
            ReferenceCountUtil.release(mqttMessage.payload());
        }
    }

    private void processQos2(ChannelHandlerContext ctx,Message innerMsg){
        log.debug("[PubMessage] -> Process qos2 message,clientId={}",innerMsg.getClientId());
        boolean flag = flowMessageStore.cacheRecMsg(innerMsg.getClientId(),innerMsg);
        if(!flag){
            log.warn("[PubMessage] -> cache qos2 pub message failure,clientId={}",innerMsg.getClientId());
        }
        MqttMessage pubRecMessage = MessageUtil.getPubRecMessage(innerMsg.getMsgId());
        ctx.writeAndFlush(pubRecMessage);
    }

    private void processQos1(ChannelHandlerContext ctx,Message innerMsg){
        processMessage(innerMsg);
        log.debug("[PubMessage] -> Process qos1 message,clientId={}",innerMsg.getClientId());
        MqttPubAckMessage pubAckMessage = MessageUtil.getPubAckMessage(innerMsg.getMsgId());
        ctx.writeAndFlush(pubAckMessage);
    }
    private void writeError(ChannelHandlerContext context,String message){
        Message innerMsg = new Message();
        innerMsg.setClientId("sys_root");
        innerMsg.setType(Message.Type.PUBLISH);
        Map<String,Object> headers = new HashMap<>();
        headers.put(MessageHeader.TOPIC,"sys");
        headers.put(MessageHeader.QOS,0);
        headers.put(MessageHeader.RETAIN,false);
        headers.put(MessageHeader.DUP,false);
        innerMsg.setHeaders(headers);
        innerMsg.setMsgId(-1);
        innerMsg.setPayload(message.getBytes(CharsetUtil.UTF_8));
        MqttMessage pubMessage = MessageUtil.getPubMessage(innerMsg,false,0,-1);
        context.writeAndFlush(pubMessage).addListener(future -> {
            if(!future.isSuccess()){
                log.error("消息回发失败");
            }
        });
    }
    private <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }
}
