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

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 消费者分组信息。
 * @author shijia.wxr
 */
public class ConsumerGroupInfo {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    //消费者分组名称
    private final String groupName;
    //订阅的多个topic以及topic的配置。
    private final ConcurrentHashMap<String/* Topic */, SubscriptionData> subscriptionTable =
            new ConcurrentHashMap<String, SubscriptionData>();

    //订阅的多个topic
    //每一个消费分组下面的消费者集合。
    private final ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable =
            new ConcurrentHashMap<Channel, ClientChannelInfo>(16);
    //消费类型
    private volatile ConsumeType consumeType;
    //消费模型。
    private volatile MessageModel messageModel;
    //消费位点从哪儿开始。
    private volatile ConsumeFromWhere consumeFromWhere;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();


    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
            ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }


    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }


    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }


    public ConcurrentHashMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }


    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<Channel>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }


    public List<String> getAllClientId() {
        List<String> result = new ArrayList<String>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }


    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }


    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn(
                "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                info.toString(), groupName);
            return true;
        }

        return false;
    }

    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
            MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) { //原来没有注册对应的通道。
            //把新的通道注册进来 , 先进入的线程能够进入if 分支。
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                    messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        }
        else { //必须是相同的clientid 才能更新消费者通道信息。
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error(
                    "[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                    this.groupName,//
                    infoOld.toString(),//
                    infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew); //clientid不同的时候 ，也认为是加入新的消费者通道信息。
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }





    /** 更新消费者分组的订阅数据（SubscriptionData）， 每一个SubscriptionData 对应一个topic .
     *
     *  这个方法比对了新旧两个topic列表， 做了删除， 新增和更新操作。
     * @param subList
     * @return
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        //先把sublist中新增加的元素和要更新的元素处理掉。
        for (SubscriptionData sub : subList) {
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            if (old == null) { //两个线程并发，只有一个prev == null
                SubscriptionData prev = this.subscriptionTable.put(sub.getTopic(), sub);
//                this.updateGroovyScriptOfTopic(sub);
                if (null == prev) { //第一次写入topic的订阅元数据。
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}", this.groupName,
                        sub.toString());
                }
            }
            else if (sub.getSubVersion() > old.getSubVersion()) { //topic已经存在， 则比较版本号，只有更大的版本号能写入。
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) { //被动消费方式下 ，日志记录新旧两个订阅配置.
                    log.info("subscription changed, group: {} OLD: {} NEW: {}", //
                        this.groupName,//
                        old.toString(),//
                        sub.toString()//
                    );
                }

                this.subscriptionTable.put(sub.getTopic(), sub);
                //脚本过滤改为client 处理， 减轻服务器的压力。
//                this.updateGroovyScriptOfTopic(sub);


            }
        }

        //把不在sublist中的订阅topic删除掉。
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {  //迭代每一个topic的订阅元数据。
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData sub : subList) { //判断老的topic在新的topic 订阅列表中存在，
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) { //老的topic 不在新的topic订阅列表中，则从新的
                log.warn("subscription changed, group: {} remove topic {} {}", //
                    this.groupName,//
                    oldTopic,//
                    next.getValue().toString()//
                );

                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }


    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }


    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }


    public ConsumeType getConsumeType() {
        return consumeType;
    }


    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }


    public String getGroupName() {
        return groupName;
    }


    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }


    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }


    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }


    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
