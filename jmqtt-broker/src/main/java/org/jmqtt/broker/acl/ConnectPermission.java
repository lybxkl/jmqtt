package org.jmqtt.broker.acl;

/**
 * Connect permission manager
 */
public interface ConnectPermission {

    /**
     * Verfy the clientId whether it meets the requirements or not
     * 确认客户是否符合要求
     */
    boolean clientIdVerfy(String clientId);

    /**
     * if the client is on blacklist,is not allowed to connect
     * 如果客户端在黑名单上，则不允许连接
     */
    boolean onBlacklist(String remoteAddr,String clientId);

    /**
     * verfy the clientId,username,password whether true or not
     * 验证clientId、用户名、密码是否正确
     */
    boolean authentication(String clientId,String userName,byte[] password);

    /**
     * verfy the client's heartbeat time whether the compliance
     * 验证客户的心跳时间是否合规
     */
    boolean verfyHeartbeatTime(String clientId,int time);
}
