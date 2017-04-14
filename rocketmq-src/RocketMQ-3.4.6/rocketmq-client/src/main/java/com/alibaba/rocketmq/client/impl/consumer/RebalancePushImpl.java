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

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * //selectOneMessageQueue  messageQueueList 是投递消息的时候对应的topic队列，每次投递的时候乱序选择队列投递，见ROCKET开发手册7.8节
 //rebalance相关的是针对消费，例如有多个消费者消费同一个topic，该topic有10个队列，则消费者1消费1-5队列，消费者2消费6-10对了，见ROCKETMQ开发手册7-5
 * @author shijia.wxr  真正使用在 DefaultMQPushConsumerImpl
 */
public class RebalancePushImpl extends RebalanceImpl {
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;


    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mQClientFactory,
            DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    //updateProcessQueueTableInRebalance-> RebalancePushImpl.dispatchPullRequest

    //在按topic做rebalance操作的时候PullRequest被分发出去, 一个PullRequest对应一个消费者分组对topic的某一个队列的消费。
    //也就是存入 PullMessageService.pullRequestQueue中
    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }

    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1;
        final ConsumeFromWhere consumeFromWhere =
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
        case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
        case CONSUME_FROM_MIN_OFFSET:
        case CONSUME_FROM_MAX_OFFSET:
        case CONSUME_FROM_LAST_OFFSET: { //从remote broker获取队列的消费位点。
            long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
            if (lastOffset >= 0) { //从其他消费者存储的那个位点开始。
                result = lastOffset;
            }
            // First start,no offset
            else if (-1 == lastOffset) { //集群第一次启动消费的时候，从broker读取不到历史位点时，  如果是分组的重试topic, 则要回溯到开始位点，
                //否则用最新的位点。
                if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    result = 0L;
                }
                else {
                    try {
                        result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                    }
                    catch (MQClientException e) {
                        result = -1;
                    }
                }
            }
            else {
                result = -1;
            }
            break;
        }
        case CONSUME_FROM_FIRST_OFFSET: {
            long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
            if (lastOffset >= 0) { //从broker读取到了历史位点，则从历史位点开始。
                result = lastOffset;
            }
            else if (-1 == lastOffset) { //从broker读取不到历史位点，则从起始位点开始。
                result = 0L;
            }
            else {
                result = -1;
            }
            break;
        }
        case CONSUME_FROM_TIMESTAMP: {
            long lastOffset = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else if (-1 == lastOffset) {
                if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    try {
                        result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                    }
                    catch (MQClientException e) {
                        result = -1;
                    }
                }
                else {
                    try {
                        /**
                         *  //从指定的时间戳开始进行位点消费， 通过时间戳定位位点的方法如下：
                         //先通过consume queue的mapfile的最后修改时间（比查找时间要大） 找到mapfile,
                         //然后从mapfile的头开始做二分查找，用consume queue的item找到commitlog中的消息，
                         //用消息的存储时间和查询时间做比对，直到精确定位，或者定位到距离查询时间最小的那个消息。
                         */
                        long timestamp =
                                UtilAll.parseDate(
                                    this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer()
                                        .getConsumeTimestamp(), UtilAll.yyyyMMddHHmmss).getTime();
                        result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                    }
                    catch (MQClientException e) {
                        result = -1;
                    }
                }
            }
            else {
                result = -1;
            }
            break;
        }

        default:
            break;
        }

        return result;
    }


    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
    }


    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq); //消费位点同步到broker .
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq); //清理本地存储的队列消费位点。
        if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            try {
                if (pq.getLockConsume().tryLock(1000, TimeUnit.MILLISECONDS)) {
                    try {
                        this.unlock(mq, true);
                        return true;
                    }
                    finally {
                        pq.getLockConsume().unlock();
                    }
                }
                else {
                    log.warn(
                        "[WRONG]mq is consuming, so can not unlock it, {}. maybe hanged for a while, {}",//
                        mq,//
                        pq.getTryUnlockTimes());

                    pq.incTryUnlockTimes();
                }
            }
            catch (Exception e) {
                log.error("removeUnnecessaryMessageQueue Exception", e);
            }

            return false;
        }
        return true;
    }


    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }
}
