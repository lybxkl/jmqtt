package org.jmqtt.broker.acl;

/**
 * publish messages and subscribe topic permisson
 */
public interface PubSubPermission {

    /**
     * verfy the clientId whether can publish message to the topic
     * 验证是否可以将消息发布到主题
     */
    boolean publishVerfy(String clientId,String topic);

    /**
     * verfy the clientId whether can subscribe the topic
     * 是否可以订阅该主题
     */
    boolean subscribeVerfy(String clientId,String topic);

}
