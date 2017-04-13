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
package com.alibaba.rocketmq.client.consumer.rebalance;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;


/**
 * Average Hashing queue algorithm
 *
 * @author manhong.yqd  Rebalance 算法实现策略
 ** AllocateMessageQueueStrategy实现在类 AllocateMessageQueueAveragely  AllocateMessageQueueByMachineRoom  AllocateMessageQueueByConfig
 * AllocateMessageQueueAveragelyByCircle  Rebalance 算法实现策略
 */

public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final Logger log = ClientLogger.getLog();


    @Override
    public String getName() {
        return "AVG";
    }

    /* 下面这个算法的目的就是看属于同一消费分组的消费则如何消费queue上面的消息
     * 返回值存放了该currentCID应该消费的队列信息，也就是currentCID应该消费list中存放的队列
    */
    @Override  //RebalanceImpl.rebalanceByTopic中调用
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", //
                    consumerGroup, //
                    currentCID,//
                    cidAll);
            return result;
        }

        /* 下面这个算法的目的就是看属于同一消费分组的消费则如何消费queue上面的消息 */

        int index = cidAll.indexOf(currentCID); //cidALL链表中内容为currentCID的索引
        int mod = mqAll.size() % cidAll.size();
        //如果消费同一个topic的相同消费者数大于队列数，则averageSize=1
        //如果消费者数小于队列数，则averageSize=队列数/消费者数
        int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }
}
