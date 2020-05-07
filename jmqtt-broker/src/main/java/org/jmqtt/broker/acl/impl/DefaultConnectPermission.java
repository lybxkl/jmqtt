package org.jmqtt.broker.acl.impl;

import org.jmqtt.broker.acl.ConnectPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultConnectPermission implements ConnectPermission {
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectPermission.class);

    @Override
    public boolean clientIdVerfy(String clientId) {
        return true;
    }

    @Override
    public boolean onBlacklist(String remoteAddr, String clientId) {
        return false;
    }

    @Override
    public boolean authentication(String clientId, String userName, byte[] password) {
        logger.info("客户端：{}，账号：{}，密码：{}",clientId,userName==null?"":userName,password==null?"":new String(password));
        if(userName==null||password==null){
            return false;
        }
        return true;
    }

    @Override
    public boolean verfyHeartbeatTime(String clientId, int time) {
        return true;
    }
}
