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
package com.alibaba.rocketmq.client.impl.producer;

import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author shijia.wxr
 */
public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    //selectOneMessageQueue  messageQueueList 是投递消息的时候对应的topic队列，每次投递的时候乱序选择队列投递，见ROCKET开发手册7.8节
    //rebalance相关的是针对消费，例如有多个消费者消费同一个topic，该topic有10个队列，则消费者1消费1-5队列，消费者2消费6-10对了，见ROCKETMQ开发手册7-5
    //fetchPublishMessageQueues->topicRouteData2TopicPublishInfo 中获取topic对应的queue信息，
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    private AtomicInteger sendWhichQueue = new AtomicInteger(0);


    public boolean isOrderTopic() {
        return orderTopic;
    }


    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }


    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }


    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }


    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }


    public AtomicInteger getSendWhichQueue() {
        return sendWhichQueue;
    }


    public void setSendWhichQueue(AtomicInteger sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }


    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }


    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    //selectOneMessageQueue  messageQueueList 是投递消息的时候对应的topic队列，每次投递的时候乱序选择队列投递，见ROCKET开发手册7.8节
    //rebalance updateProcessQueueTableInRebalance相关的是针对消费，例如有多个消费者消费同一个topic，该topic有10个队列，则消费者1消费1-5队列，消费者2消费6-10对了，见ROCKETMQ开发手册7-5
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName != null) {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }

            return null;
        }
        else {
            int index = this.sendWhichQueue.getAndIncrement();
            int pos = Math.abs(index) % this.messageQueueList.size();
            return this.messageQueueList.get(pos);
        }
    }


    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
                + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }
}
