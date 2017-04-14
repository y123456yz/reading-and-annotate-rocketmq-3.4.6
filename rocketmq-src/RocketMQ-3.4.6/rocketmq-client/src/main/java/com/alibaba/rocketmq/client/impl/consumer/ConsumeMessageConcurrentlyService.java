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

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.CMResult;
import com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;


/**
 * @author shijia.wxr
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue; //工作隊列
    private final ThreadPoolExecutor consumeExecutor; //線程池
    private final String consumerGroup;

    //创建只有一条线程的线程池，他可以在指定延迟后执行线程任务  ScheduledExecutorService定时周期执行指定的任务,线程真正运行在submitConsumeRequestLater
    private final ScheduledExecutorService scheduledExecutorService;

    /* 線程池初始化 */
    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(//
            this.defaultMQPushConsumer.getConsumeThreadMin(),//
            this.defaultMQPushConsumer.getConsumeThreadMax(),//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.consumeRequestQueue,//
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        //创建只有一条线程的线程池，他可以在指定延迟后执行线程任务  ScheduledExecutorService定时周期执行指定的任务 线程真正运行submitConsumeRequestLater
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                    "ConsumeMessageScheduledThread_"));
    }


    public void start() { /* 線程運行實際上在下面的類 ConsumeRequest */
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
    }


    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }


    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        //// ConsumeMessageConcurrentlyService.submitConsumeRequest  msgs来源在这里
        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }


        @Override
        public void run() { ////ConsumeMessageConcurrentlyService.submitConsumeRequest 中触发执行
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped {}",
                    this.messageQueue);
                return;
            }

            /* 这里的listener也就是consumer或者proxy中registerMessageListener的lister */
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext
                    .setConsumerGroup(ConsumeMessageConcurrentlyService.this.defaultMQPushConsumer
                        .getConsumerGroup());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl
                    .executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();

            /* 业务在这里消费消息，并返回消费结果，RECONSUME_OK,失败返回 RECONSUME_LATER */
            try {
                ConsumeMessageConcurrentlyService.this.resetRetryTopic(msgs);
                //业务在这里开始消费消息  例如 pullConsumer类中的consumer.registerMessageListener()回调就是在这里执行
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            }
            catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",//
                    RemotingHelper.exceptionSimpleDesc(e),//
                    ConsumeMessageConcurrentlyService.this.consumerGroup,//
                    msgs,//
                    messageQueue);
            }

            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            if (null == status) { //如果业务consumer的返回值为空，则默认需要继续消费
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",//
                    ConsumeMessageConcurrentlyService.this.consumerGroup,//
                    msgs,//
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl
                    .executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager().incConsumeRT(
                ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            if (!processQueue.isDropped()) {
                //对业务处理返回的status CONSUME_SUCCESS或者RECONSUME_LATER做相应的处理
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            }
            else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}",
                    messageQueue, msgs);
            }
        }


        public List<MessageExt> getMsgs() {
            return msgs;
        }


        public ProcessQueue getProcessQueue() {
            return processQueue;
        }


        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }

    //ConsumeMessageConcurrentlyService.processConsumeResult中执行
    //发送消息到broker，例如存入重试队列
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue()
                .getBrokerName());
            return true;
        }
        catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(),
                e);
        }

        return false;
    }


    /**
     *
     * 整个这段代码的思路：
     * 如果ConsumeConcurrentlyStatus 标识的业务消费状态是成功，则broker上队列的offset位点前移（一批消息的最大位点+1）
     * 如果业务消费失败，则把这批消息重新投递回broker （这批投回去的消息会延迟投递回来，即有一个backoff机制。）
     * ， 这里又分成两种情况：
     * 1） 重新投递回去成功， 队列的offset仍然前移。
     * 2） 重新投递回去失败， 则把这批重投失败的消息继续交给业务处理，与此同时， 队列的位点会在重投失败的消息中选择一个小的位点。
     * 比如1,2,3,4,5 中2，3重投失败，则位点会变成2， 这里就存在一个重复消费的问题（本身rocketmq不保证消息不重， 但一定不丢。）
     *
     * 从这里看出，虽然该msg消费失败了，但是只要把该msg打回broker成功，则还是会更新该msg的消费位点向后移动的，当broker收到这条
     * 打回成功的消息后，会重新写一条消息到broker中，下次客户端就会再次拉取这条打回的消息，之前消费失败的消息由于移动了其offset，因此
     * 下次不会再次拉取该消息。
     *
     * 定期通知broker进行位点更新见//MQClientInstance.startScheduledTask->persistConsumerOffset->MQClientInstance.startScheduledTask->persistConsumerOffset
     *
     * @param status
     * @param context
     * @param consumeRequest
     */
    public void processConsumeResult(//ConsumeRequest.run中执行
            final ConsumeConcurrentlyStatus status, //
            final ConsumeConcurrentlyContext context, //
            final ConsumeRequest consumeRequest//
    ) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {
        case CONSUME_SUCCESS:
            if (ackIndex >= consumeRequest.getMsgs().size()) {
                ackIndex = consumeRequest.getMsgs().size() - 1;
            }
            int ok = ackIndex + 1;
            int failed = consumeRequest.getMsgs().size() - ok;
            /* 业务消费完消息后做相应的统计 */
            this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup,
                consumeRequest.getMessageQueue().getTopic(), ok);
            this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                consumeRequest.getMessageQueue().getTopic(), failed);
            break;
        case RECONSUME_LATER:
            ackIndex = -1;
            this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                consumeRequest.getMessageQueue().getTopic(), consumeRequest.getMsgs().size());
            break;
        default:
            break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
        case BROADCASTING:
            for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                MessageExt msg = consumeRequest.getMsgs().get(i);
                log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
            }
            break;
        case CLUSTERING:
            //下面这段代码的意思是： 如果业务方消息消费失败(ackIndex = -1) ，那么就把这批已经拉到的消费失败的消息重新打回broker
            List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
            for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) { //注意这里i是从ackIndex + 1开始
                MessageExt msg = consumeRequest.getMsgs().get(i);
                //把消费失败的消息重新打回broker . 存入broker的重试队列

                boolean result = this.sendMessageBack(msg, context); //注意这里面打回broker的时候topic变为 RETRY_GROUP_TOPIC_PREFIX + ConsumerGroup
                if (!result) {
                    //这里的赋值实际上在上面的sendMessageBack中会用到
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1); //该msg需要客户端再次消费， 第一条消息加1，第二天失败的就是+2,以此类推
                    msgBackFailed.add(msg); //打回broker失败，则加入msgBackFailed链表
                }
            }
            //如果消费失败的消息重新打回broker仍然失败，则把这些msgBackFailed从msgs链表去除，同时提交给一个调度线程池， 间隔5秒以后，再次消息。
            if (!msgBackFailed.isEmpty()) {
                consumeRequest.getMsgs().removeAll(msgBackFailed);

                //过5s钟再次推送这些打回broker失败的消息给业务消费，因为
                this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(),
                    consumeRequest.getMessageQueue());
            }
            break;
        default:
            break;
        }

        //如果消费失败的消息重新打回broker 仍然失败， 即前面的msgBackFailed 非空，那么removeMessage 这个函数
        //得到的位点offset就是一批消息中最小的那个位点，而如果msgBackFailed 为空，则这里offset 就是这批消息的最大位点+1

        //getMsgs()获取到的msgs链表中存储的是消费失败打回broker成功的msg和消费成功的msg
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0) {
            //客户端定时通知broker中的topic queue进行位点更新
            //位点更新，见MQClientInstance.startScheduledTask->persistConsumerOffset->MQClientInstance.startScheduledTask->persistConsumerOffset
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(),
                offset, true);
        }
    }

    //5ms后再次推消息给业务消费
    private void submitConsumeRequestLater(//
            final List<MessageExt> msgs, //
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue//
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue,
                    true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    //DefaultMQPushConsumerImpl.pullMessage.PullCallback.onSuccess中执行，真正的接口在 ConsumeMessageConcurrentlyService.submitConsumeRequest
    @Override
    public void submitConsumeRequest(//
            final List<MessageExt> msgs, //
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue, //
            final boolean dispatchToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) { //如果msg数小于consumeBatchSize，则一起消费  ????? 这里应该要加上msgs.size() > 0
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);//线程池执行 ConsumeRequest.run
        }
        else { //超过一次最大批量消费的消息数， 则分成几批处理。
            for (int total = 0; total < msgs.size();) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    }
                    else {
                        break;
                    }
                }
                //
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                this.consumeExecutor.submit(consumeRequest); //触发执行 ConsumeRequest.run
            }
        }
    }


    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0 //
                && corePoolSize <= Short.MAX_VALUE //
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }


    @Override
    public void incCorePoolSize() {
//        long corePoolSize = this.consumeExecutor.getCorePoolSize();
//        if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
//            this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize() + 1);
//        }
//
//        log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup: {}", //
//            corePoolSize,//
//            this.consumeExecutor.getCorePoolSize(),//
//            this.consumerGroup);
    }


    @Override
    public void decCorePoolSize() {
//        long corePoolSize = this.consumeExecutor.getCorePoolSize();
//        if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin()) {
//            this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize() - 1);
//        }
//
//        log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup: {}", //
//            corePoolSize,//
//            this.consumeExecutor.getCorePoolSize(),//
//            this.consumerGroup);
    }


    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }


    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.resetRetryTopic(msgs);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new messge: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                case CONSUME_SUCCESS:
                    result.setConsumeResult(CMResult.CR_SUCCESS);
                    break;
                case RECONSUME_LATER:
                    result.setConsumeResult(CMResult.CR_LATER);
                    break;
                default:
                    break;
                }
            }
            else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        }
        catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",//
                RemotingHelper.exceptionSimpleDesc(e),//
                ConsumeMessageConcurrentlyService.this.consumerGroup,//
                msgs,//
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }


    public void resetRetryTopic(final List<MessageExt> msgs) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }
        }
    }
}
