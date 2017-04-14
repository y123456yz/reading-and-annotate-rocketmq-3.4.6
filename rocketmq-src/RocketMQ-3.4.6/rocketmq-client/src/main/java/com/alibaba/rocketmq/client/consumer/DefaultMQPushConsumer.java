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
package com.alibaba.rocketmq.client.consumer;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Wrapped push consumer.in fact,it works as remarkable as the pull consumer
 *
 * @author shijia.wxr
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl; /* DefaultMQPushConsumer中赋值 */
    /**
     * Do the same thing for the same Group, the application must be set,and
     * guarantee Globally unique //DefaultMQPushConsumer中赋值
     */
    private String consumerGroup;
    /**
     * Consumption pattern,default is clustering
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Consumption offset
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    /**
     * Backtracking consumption time with second precision.time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));
    /**
     * Queue allocation algorithm  //DefaultMQPushConsumer中赋值
     * Rebalance 算法实现策略
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship  订阅关系
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();
    /**
     * Message listener
     * 就是PushConsumer main函数中的consumer.registerMessageListener(new MessageListenerConcurrently()，这里的
     * new MessageListenerConcurrently()  消息监听器
     */
    private MessageListener messageListener; //赋值见registerMessageListener  MessageListenerConcurrently
    /**
     * Offset Storage 消费进度存储
     */
    private OffsetStore offsetStore;
    /**
     * Minimum consumer thread number  消费线程池数量
     */
    private int consumeThreadMin = 20;
    /**
     * Max consumer thread number 消费线程池数量
     */
    private int consumeThreadMax = 64;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     *单队列并行消费允许的最大跨度
     * 非顺序消费时，消息的消费位点的最大差值。
     */
    private int consumeConcurrentlyMaxSpan = 2000;
    /**
     * Flow control threshold 拉消息本地队列缓存消息最大数
     */
    private int pullThresholdForQueue = 1000;
    /**
     * Message pull Interval
     * 拉消息间隔，由于是长轮询，所以
     为 0，但是如果应用为了流控，也
     可以设置大于 0 的值，单位毫秒
     */
    private long pullInterval = 0;
    /**
     * Batch consumption size 批量消费，一次消费多少条消息
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * Batch pull size 批量拉消息，一次最多拉多少条
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;


    public DefaultMQPushConsumer() {
        this(MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }


    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }


    public DefaultMQPushConsumer(final String consumerGroup) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    /**
     *
     * @param consumerGroup 消费者分组
     * @param rpcHook 给broker发送请求的钩子，可以在请求前后植入拦截处理。
     * @param allocateMessageQueueStrategy 消费者分担topic下队列的策略，默认是均摊消费(大致）
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQPushConsumerImpl.viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException,
            InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }


    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }


    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }


    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }


    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }


    /**
     * 指定位点从哪儿开始。
     * @param consumeFromWhere
     */
    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }


    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }


    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }


    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }


    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }


    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }


    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }


    public MessageListener getMessageListener() {
        return messageListener;
    }


    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    /**
     * 指定消费模式是集群还是广播消费。
     * @param messageModel
     */
    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }


    public int getPullBatchSize() {
        return pullBatchSize;
    }


    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }


    public long getPullInterval() {
        return pullInterval;
    }


    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }


    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }


    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }


    public Map<String, String> getSubscription() {
        return subscription;
    }


    public void setSubscription(Map<String, String> subscription) {
        this.subscription = subscription;
    }


    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }


    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(topic);
    }


    @Override
    public void start() throws MQClientException {
        this.defaultMQPushConsumerImpl.start();
    }


    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown();
    }


    @Override
    @Deprecated
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }


    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }


    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }


    /**
     * 订阅topic .
     * @param topic 消息主题
     * @param subExpression 只支持有限的过滤表达式，  比如： * 或者 null 意味着所有topic的消息都被订阅。
     *                          tagA || tagB 用竖线分割，标识订阅某几个tags .
     *            subscription expression.it only support or operation such as
     *            "tag1 || tag2 || tag3" <br>
     *            if null or * expression,meaning subscribe all
     * @throws MQClientException
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, subExpression);
    }


    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource);
    }


    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }


    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }


    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }


    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }


    public OffsetStore getOffsetStore() {
        return offsetStore;
    }


    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }


    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }


    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }


    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }


    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }


    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }
}
