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
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.Validators;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.alibaba.rocketmq.client.hook.ConsumeMessageHook;
import com.alibaba.rocketmq.client.hook.FilterMessageHook;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.common.protocol.body.ConsumeStatus;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.body.ProcessQueueInfo;
import com.alibaba.rocketmq.common.protocol.body.QueueTimeSpan;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import org.slf4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author shijia.wxr  一个消费分组对应一个MQConsumerInner，存储在 MQClientInstance.consumerTable
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur  拉取消息异常，延迟这么久消费
     */
    private static final long PullTimeDelayMillsWhenException = 3000;
    /**
     * Flow control interval //拉取消息的流控, 处理队列中的消息超过1000 ，等50秒再执行。
     */
    private static final long PullTimeDelayMillsWhenFlowControl = 50;
    /**
     * Delay some time when suspend pull service   pause情况下延迟时间
     */
    private static final long PullTimeDelayMillsWhenSuspend = 1000;
    private static final long BrokerSuspendMaxTimeMillis = 1000 * 15;
    private static final long ConsumerTimeoutMillisWhenSuspend = 1000 * 30;
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory; //本类的start接口中赋值
    private PullAPIWrapper pullAPIWrapper;
    private volatile boolean pause = false;
    private boolean consumeOrderly = false;
    /*
    * 就是PushConsumer main函数中的consumer.registerMessageListener(new MessageListenerConcurrently()，这里的
    * new MessageListenerConcurrently()
    */
    private MessageListener messageListenerInner; //赋值见 registerMessageListener //MessageListenerConcurrently
    //位点更新见 ConsumeMessageConcurrentlyService.processConsumeResult
    private OffsetStore offsetStore; //本類的start接口賦值 RemoteBrokerOffsetStore
    private ConsumeMessageService consumeMessageService; //赋值见该类的start接口  ConsumeMessageConcurrentlyService

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private final long consumerStartTimestamp = System.currentTimeMillis();


    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    private final RPCHook rpcHook;


    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
    }


    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }


    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }


    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                }
                catch (Throwable e) {
                }
            }
        }
    }


    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                }
                catch (Throwable e) {
                }
            }
        }
    }


    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return result;
    }


    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }


    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }


    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }


    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }


    public OffsetStore getOffsetStore() {
        return offsetStore;
    }


    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }


    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }


    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }


    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }


    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        //SubscriptionData 集合
        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    //调用源头在 MQClientInstance.start(this.rebalanceService.start();)中执行
    @Override //MQClientInstance.doRebalance中执行
    public void doRebalance() {
        if (this.rebalanceImpl != null) {//真正做rebalance处理在 RebalanceImpl.doRebalance
            this.rebalanceImpl.doRebalance();
        }
    }


    @Override
    //MQClientInstance.startScheduledTask->persistConsumerOffset->MQClientInstance.startScheduledTask->persistConsumerOffset中执行
    //当客户端消费成功后，需要把该信息推送给broker，这样broker才能更新offset中执行  当客户端消费成功后，需要把该信息推送给broker，这样broker才能更新offset
    public void persistConsumerOffset() { //DefaultMQPushConsumerImpl.persistConsumerOffset
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            if (allocateMq != null) {
                mqs.addAll(allocateMq);
            }

            this.offsetStore.persistAll(mqs);
        }
        catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup()
                    + " persistConsumerOffset exception", e);
        }
    }

    ////MQClientInstance.updateTopicRouteInfoFromNameServer 中执行
    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                //不同broker上面的队列号都是从0 - readQueueNums，队列号可能重复，但是他们的brakername不一样，见topicRouteData2TopicSubscribeInfo
                //把该topic及其对应的 queue信息添加到 RebalanceImpl.topicSubscribeInfoTable
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }


    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }


    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }


    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    private long flowControlTimes1 = 0;
    private long flowControlTimes2 = 0;


    /**
     * 消费者分组从指定的消费队列(MessageQueue)拉取消息（一个PullRequest 对应一个消费者分组对topic的一个队列的消费。）
     * 在按topic 做rebalance操作的时候会被触发一次。
     *
     * 所以， 这里总结下来，每一个消费者分组对topic的某一个队列进行消费，是通过rebalance操作来触发的，
     * 而rebalance操作又是由消费者的加入，退出，订阅和取消订阅来触发的。
     * 一旦消费了队列， 其实就是拉一批消费一批再拉下一批，循环往复。
     *
     *
     * @param pullRequest
     */ //PullMessageService.run线程调度走到这里
    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) { //在消费前先判断处理队列是否已经被drop ,因为在可能正好被rebalance操作drop掉了。
            //RebalancePushImpl 368行
            log.info("the pull request[{}] is droped.", pullRequest.toString());
            return;
        }

        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        }
        catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
            return;
        }

        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}",
                this.defaultMQPushConsumer.getInstanceName());
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenSuspend);
            return;
        }

        long size = processQueue.getMsgCount().get();
        //拉取消息的流控, 处理队列中的消息超过1000 ，等50秒再执行。
        if (size > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
            if ((flowControlTimes1++ % 1000) == 0) {   //流控每超过1000次 ，写一次警告日志。
                log.warn("the consumer message buffer is full, so do flow control, {} {} {}", size,
                    pullRequest, flowControlTimes1);
            }
            return;
        }

        if (!this.consumeOrderly) { //非顺序消息的最大和最小消费位点差值超过一定量，则50ms后再处理。 。
            /* 本地积压消息数超过限制 */
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
                if ((flowControlTimes2++ % 1000) == 0) { //流控每超过1000次 ，写一次警告日志。
                    log.warn("the queue's messages, span too long, so do flow control, {} {} {}",
                        processQueue.getMaxSpan(), pullRequest, flowControlTimes2);
                }
                //如果从该 pullRequest 对应的queue中拉取到的消息数超过了consumeConcurrentlyMaxSpan阈值则延迟 PullTimeDelayMillsWhenFlowControl s再次拉取，
                //这里return后就不会执行下面流程中的拉取操作了
                return;
            }
        }

        final SubscriptionData subscriptionData =
                this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        //該類在下面的this.pullAPIWrapper.pullKernelImpl()使用該類
        PullCallback pullCallback = new PullCallback() {
            @Override
            /* new一個pullCallback類并對onException接口和onSuccess接口重寫 */
            public void onSuccess(PullResult pullResult) { //真正执行是拉取消息成功后在 pullMessageAsync 中执行
                if (pullResult != null) {
                    //解析出的msg存入msgFoundList
                    pullResult =
                            DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(
                                pullRequest.getMessageQueue(), pullResult, subscriptionData);

                    switch (pullResult.getPullStatus()) {
                    case FOUND: //有拉取到消息
                        //上一次拉取的cq的起始位点。
                        long prevRequestOffset = pullRequest.getNextOffset();
                        //下一次拉取的cq的起始位点。
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(
                            pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(), pullRT);

                        long firstMsgOffset = Long.MAX_VALUE;
                        if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        }
                        else {
                            firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                            /* 本次从broker拉取了size调消息过来，记录下统计信息 */
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(
                                pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(),
                                pullResult.getMsgFoundList().size());

                            boolean dispathToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                            DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(//
                                pullResult.getMsgFoundList(), //
                                processQueue, //
                                pullRequest.getMessageQueue(), //
                                dispathToConsume);

                            //间隔指定的时间，或者立即再拉。
                            if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                    DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                            }
                            else {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            }
                        }

                        if (pullResult.getNextBeginOffset() < prevRequestOffset//
                                || firstMsgOffset < prevRequestOffset) {
                            log.warn(
                                "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",//
                                pullResult.getNextBeginOffset(),//
                                firstMsgOffset,//
                                prevRequestOffset);
                        }

                        break;
                    case NO_NEW_MSG:
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                        DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case NO_MATCHED_MSG:
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                        DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case OFFSET_ILLEGAL:
                        log.warn("the pull request offset illegal, {} {}",//
                            pullRequest.toString(), pullResult.toString());
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                        pullRequest.getProcessQueue().setDropped(true);
                        DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(
                                        pullRequest.getMessageQueue(), pullRequest.getNextOffset(), false);

                                    DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest
                                        .getMessageQueue());

                                    DefaultMQPushConsumerImpl.this.rebalanceImpl
                                        .removeProcessQueue(pullRequest.getMessageQueue());

                                    log.warn("fix the pull request offset, {}", pullRequest);
                                }
                                catch (Throwable e) {
                                    log.error("executeTaskLater Exception", e);
                                }
                            }
                        }, 10000);
                        break;
                    default:
                        break;
                    }
                }
            }


            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }
                //有拉取消息的异常，延迟3每秒重新拉。
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                    PullTimeDelayMillsWhenException);
            }
        };

        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) { //集群消费模式，
            //RemoteBrokerOffsetStore.readOffset
            commitOffsetValue =
                    this.offsetStore.readOffset(pullRequest.getMessageQueue(),
                        ReadOffsetType.READ_FROM_MEMORY); //内存位点其实还是从broker同步得到的。
            if (commitOffsetValue > 0) { //拉取过一次消息以后，就启动自动提交消费位点到broker.
                commitOffsetEnable = true;
            }
        }

        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd =
                this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) { //  在没有class过滤并且在拉取以后提交一次订阅表达式， 则给订阅表达式赋值。
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        int sysFlag = PullSysFlag.buildSysFlag(//
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null,// subscription
            classFilter // class filter
            );
        try { //开始从broker对应的brokerAddr异步拉取消息
            this.pullAPIWrapper.pullKernelImpl(//
                pullRequest.getMessageQueue(), // 1
                subExpression, // 2
                subscriptionData.getSubVersion(), // 3
                pullRequest.getNextOffset(), // 4
                this.defaultMQPushConsumer.getPullBatchSize(), // 5
                sysFlag, // 6
                commitOffsetValue,// 7
                BrokerSuspendMaxTimeMillis, // 8
                ConsumerTimeoutMillisWhenSuspend, // 9
                CommunicationMode.ASYNC, // 10
                pullCallback// 11
                );
        }
        catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
        }
    }

    // updateProcessQueueTableInRebalance-> RebalancePushImpl.dispatchPullRequest
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }


    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }


    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }


    public boolean isPause() {
        return pause;
    }


    public void setPause(boolean pause) {
        this.pause = pause;
    }


    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        }
    }


    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    //registerMessageListener中调用
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }


    public void resume() {
        this.pause = false;
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }
    //结合SendMessageProcessor.consumerSendMsgBack阅读
    // DefaultMQPushConsumerImpl.sendMessageBack
    //注意这里会修改topic，修改后的topic为 RETRY_GROUP_TOPIC_PREFIX + ConsumerGroup
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr =
                    (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                            : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            //结合结合SendMessageProcessor.consumerSendMsgBack阅读
            //消费失败，重新打回消息到broker中   这里发送的报文的code:CONSUMER_SEND_MSG_BACK，对端收到后，会创建重试队列RETRY_GROUP_TOPIC_PREFIX + consumer
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000);
        }

        catch (Exception e) { //消费失败的消息打回重试队列失败，，需要重新发送到重试队列
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            //这里发送的报文code默认为code:SEND_MESSAGE，因此需要带上重试队列名，对于broker来说就相当于收到了一条发往RETRY_GROUP_TOPIC_PREFIX + consumer的消息
            Message newMsg =
                    //修改topic，修改后的topic为 RETRY_GROUP_TOPIC_PREFIX + consumer  需要重新发送
                    new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()),
                        msg.getBody());

            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId()
                    : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            int reTimes = msg.getReconsumeTimes() + 1;
            MessageAccessor.setReconsumeTime(newMsg, reTimes + "");
            newMsg.setDelayTimeLevel(3 + reTimes);

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        }
    }

    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.consumeMessageService.shutdown();
            this.persistConsumerOffset();
            this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
            this.mQClientFactory.shutdown();
            log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
            this.rebalanceImpl.destroy();
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }

    //ProxyServer.doSubscribe->consumer.start
    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}",
                this.defaultMQPushConsumer.getConsumerGroup(), this.defaultMQPushConsumer.getMessageModel(),
                this.defaultMQPushConsumer.isUnitMode());
            this.serviceState = ServiceState.START_FAILED;

            this.checkConfig();
            this.copySubscription();  //DefaultMQPushConsumerImpl.start.copySubscription

            if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                this.defaultMQPushConsumer.changeInstanceNameToPID();
            }

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPushConsumer,
                        this.rpcHook);

            this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
            this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
            this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer
                .getAllocateMessageQueueStrategy());
            this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
            this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

            if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
            }
            else {
                switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING: //广播消费模式， 位点默认存储在本地。
                    this.offsetStore =
                            new LocalFileOffsetStore(this.mQClientFactory,
                                this.defaultMQPushConsumer.getConsumerGroup());
                    break;
                case CLUSTERING: //集群消费模式， 位点默认存储在broker .位点从broker拉取到client以后会缓存在本地。
                    this.offsetStore =
                            new RemoteBrokerOffsetStore(this.mQClientFactory,
                                this.defaultMQPushConsumer.getConsumerGroup());
                    break;
                default:
                    break;
                }
            }
            this.offsetStore.load(); /* RemoteBrokerOffsetStore->load 啥也沒干 */
            /* push consumer模式的new對象在consumer.registerMessageListener(new MessageListenerConcurrently() { */
            if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                this.consumeOrderly = true;
                this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this,
                            (MessageListenerOrderly) this.getMessageListenerInner());
            }
            else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                this.consumeOrderly = false;
                /* new一个ConsumeMessageConcurrentlyService类 */
                this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this,
                            (MessageListenerConcurrently) this.getMessageListenerInner());
            }

            this.consumeMessageService.start(); //ConsumeMessageConcurrentlyService->start为空，不执行任何操作

            boolean registerOK =
                    mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                this.consumeMessageService.shutdown();
                throw new MQClientException("The consumer group["
                        + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please."
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            //ProxyServer.doSubscribe->consumer.start->mQClientFactory.start
            mQClientFactory.start(); /* 真正的客戶端服務端通信在該接口 */
            log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
            this.serviceState = ServiceState.RUNNING;
            break;
        case RUNNING:
        case START_FAILED:
        case SHUTDOWN_ALREADY:
            throw new MQClientException("The PushConsumer service state not OK, maybe started once, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        default:
            break;
        }

        //从NS获取每一个topic的路由信息，并更新pub  ,sub信息。
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        //给broker 发送producer ,consumer的心跳信息。
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        //因为有新的消费者加入，触发一次。
        this.mQClientFactory.rebalanceImmediately();
    }


    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException("consumerGroup is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException("consumerGroup can not equal "//
                    + MixAll.DEFAULT_CONSUMER_GROUP //
                    + ", please specify another one."//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException("messageModel is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException("consumeFromWhere is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.yyyyMMddHHmmss);
        if (null == dt) {
            throw new MQClientException("consumeTimestamp is invalid, yyyyMMddHHmmss" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException("allocateMessageQueueStrategy is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException("subscription is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException("messageListener is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently =
                this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently" //
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1 //
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000//
                || this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer
                    .getConsumeThreadMax()//
        ) {
            throw new MQClientException("consumeThreadMin Out of range [1, 1000]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException("consumeThreadMax Out of range [1, 1000]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1
                || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException("pullThresholdForQueue Out of range [1, 65535]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0
                || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException("pullInterval Out of range [0, 65535]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1
                || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException("pullBatchSize Out of range [1, 1024]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }
    }

    /* 注意copySubscription和subscribe的区别 */
    private void copySubscription() throws MQClientException {
        try {
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData =
                            FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                                topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                break;
            case CLUSTERING:
                final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                SubscriptionData subscriptionData =
                        FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                            retryTopic, SubscriptionData.SUB_ALL);
                this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                break;
            default:
                break;
            }
        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }


    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }


    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData =
                    FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                        topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /* 注意copySubscription和subscribe的区别 */
    public void subscribe(String topic, String fullClassName, String filterClassSource)
            throws MQClientException {
        try {
            SubscriptionData subscriptionData =
                    FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                        topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }


    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }


    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }


    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }


    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }


    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }


    public void resetOffsetByTimeStamp(long timeStamp) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }


    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }


    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }


    public ServiceState getServiceState() {
        return serviceState;
    }


    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }


    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable =
                this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }


    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }


    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE,
            String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP,
            String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it =
                this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus =
                    this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(),
                        sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }


    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }


    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic) throws RemotingException,
            MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData =
                this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic,
                    groupName(), 3000l));
        }

        return queueTimeSpan;
    }


    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }


    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }
}
