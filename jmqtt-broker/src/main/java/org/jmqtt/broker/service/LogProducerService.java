/**
 * Copyright 2018-2020 stylefeng & fengshuonan (sn93@qq.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jmqtt.broker.service;

import org.jmqtt.common.bean.Message;

/**
 * 发送日志到消息队列的接口类
 *
 * @author lyb
 * @date 2020-04-25 10:37
 */
public interface LogProducerService {

    /**
     * 发送日志
     *
     * @author lyb
     * @date 2020-04-25 10:37
     */
    void sendMsg(Message log);

}
