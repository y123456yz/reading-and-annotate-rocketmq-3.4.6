/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.broker.client;

import com.alibaba.rocketmq.broker.BrokerController;
import io.netty.channel.Channel;

import java.util.List;


/**  和客户端消费的rebalance操作有关。消费者分组中有消费者增加和退出，或者topic有删除和增加时， 通知client做rebalance .
 * @author shijia.wxr
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private final BrokerController brokerController;


    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public void consumerIdsChanged(String group, List<Channel> channels) {
        if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
            for (Channel chl : channels) {
                this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
            }
        }
    }
}
