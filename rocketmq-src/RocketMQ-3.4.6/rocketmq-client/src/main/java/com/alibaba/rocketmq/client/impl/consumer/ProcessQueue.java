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

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * 队列消费快照， 用于client处理消费队列。
 * Queue consumption snapshot
 * //每一个消息队列，对应一个处理队列。 RebalanceImpl.processQueueTable(MessageQueue----ProcessQueue)
 * @author shijia.wxr
 */
public class ProcessQueue {
    public final static long RebalanceLockMaxLiveTime = Long.parseLong(System.getProperty(
        "rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long RebalanceLockInterval = Long.parseLong(System.getProperty(
        "rocketmq.client.rebalance.lockInterval", "20000"));

    private final Logger log = ClientLogger.getLog();
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    //DefaultMQPushConsumerImpl.pullMessage->PullCallback.onSuccess->ProcessQueue.putMessage中把获取到的msg存入到msgTreeMap中
    //queueOffset<-->MessageExt  从broker拉取的消息存到本地，也就是存入msgTreeMap   接收到的消息做规则匹配满足后，会加入到ProcessQueue.msgTreeMap
    //拉取到消息后首先存入PullResult.msgFoundList，然后进行规则匹配，匹配的msg会进一步存入ProcessQueue.msgTreeMap,
    // 当业务消费msg结果打回broker后，都会移除该msg，见removeMessage，如果消费msg结果打回broker失败，则这些msg还是会留在本地msgTreeMap,进行重新发送，见processConsumeResult.submitConsumeRequestLater
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    /* 每从broker获取到消息，就要移动该offset，也就是从broker拉取到消息的最大位点 */
    private volatile long queueOffsetMax = 0L;
    /* 该ProcessQueue中有效的msg数,见本类的 putMessage 接口 */
    private final AtomicLong msgCount = new AtomicLong();
    //例如之前某个某个topicxx在broker-A和broker-B上面都有创建topicxx，现在把broker-A下线或者配置为只消费模式，则broker-A上面的topicxx对应的queue队列会被标记为dropped
    //RebalanceImpl.updateProcessQueueTableInRebalance 中更新该标记
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private final static long PullMaxIdleTime = Long.parseLong(System.getProperty(
        "rocketmq.client.pull.pullMaxIdleTime", "120000"));

    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    private final Lock lockConsume = new ReentrantLock();

    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile boolean consuming = false;
    private final TreeMap<Long, MessageExt> msgTreeMapTemp = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    /* broker队列积压的消息数，赋值见ProcessQueue.putMessage */
    private volatile long msgAccCnt = 0;


    public boolean isLockExpired() {
        boolean result = (System.currentTimeMillis() - this.lastLockTimestamp) > RebalanceLockMaxLiveTime;
        return result;
    }


    public boolean isPullExpired() {
        boolean result = (System.currentTimeMillis() - this.lastPullTimestamp) > PullMaxIdleTime;
        return result;
    }

    //DefaultMQPushConsumerImpl.pullMessage->PullCallback.onSuccess->ProcessQueue.putMessage中把获取到的msg存入到msgTreeMap中
    //匹配的msg会存入到 ProcessQueue.msgTreeMap队列
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    //把拉取到的消息存入msgTreeMap中
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        validMsgCnt++;
                        /* 更新队列queueoffset */
                        this.queueOffsetMax = msg.getQueueOffset();
                    }
                }
                //msg数增加validMsgCnt
                msgCount.addAndGet(validMsgCnt);

                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1); //获取最后一条消息
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        //在消息属性PROPERTY_MAX_OFFSET中记录了队列的最大位点， 和当前拉取到的最后一条消息
                        //的位点做差值，就是broker这个队列还堆积了多少消息未消费。。  messageExt.getQueueOffset()表示从队列中取到的最后一条消息
                        //该getQueueOffset对应的就是从队列中拉取到的最后一条消息的offset，也就是该消费分组消费到队列中的那条offset最大的消息
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }


    /**
     * 本地消息处理队列中最大和最小位点的差值 。也就是积压在本地的msg数
     * @return
     */
    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            }
            finally {
                this.lockTreeMap.readLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    //如果消费失败的消息重新打回broker 仍然失败， 即前面的msgBackFailed 非空，那么removeMessage 这个函数
    //得到的位点offset就是一批消息中最小的那个位点，而如果msgBackFailed 为空，则这里offset 就是这批消息的最大位点+1
    //当业务消费msg成功或者消费失败重新打回broker重试队列，都会移除该msg，在ConsumeMessageConcurrentlyService.processConsumeResult
    public long removeMessage(final List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                /* 这里的msgTreeMap是从broker中拉取到的所有消息，这里移除的msg是消费后通知broker成功的消息，则移除这些msg后就是通知broker失败的消息 */
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1; //默认是从broker拉取到的这批消息中最大位点+1
                    int removedCnt = 0;
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) { //每移除一个msg，则removedCnt减1，removedCnt默认值为0，每取出一个就-1成为负数
                            removedCnt--;
                        }
                    }
                    msgCount.addAndGet(removedCnt); //减去移除的msg

                    if (!msgTreeMap.isEmpty()) { //获取第一个msg的offset，也就是这批msg数据的最小offset
                        result = msgTreeMap.firstKey();
                    }
                }
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }


    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }


    public AtomicLong getMsgCount() {
        return msgCount;
    }

    //例如之前某个某个topicxx在broker-A和broker-B上面都有创建topicxx，现在把broker-A下线或者配置为只消费模式，则broker-A上面的topicxx对应的queue队列会被标记为dropped
    public boolean isDropped() {
        return dropped;
    }

    //RebalanceImpl.updateProcessQueueTableInRebalance 中调用
    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }


    public boolean isLocked() {
        return locked;
    }


    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.msgTreeMapTemp);
                this.msgTreeMapTemp.clear();
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }


    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                Long offset = this.msgTreeMapTemp.lastKey();
                msgCount.addAndGet(this.msgTreeMapTemp.size() * (-1));
                this.msgTreeMapTemp.clear();
                if (offset != null) {
                    return offset + 1;
                }
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }


    public void makeMessageToCosumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.msgTreeMapTemp.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    public List<MessageExt> takeMessags(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            msgTreeMapTemp.put(entry.getKey(), entry.getValue());
                        }
                        else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }


    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.msgTreeMapTemp.clear();
                this.msgCount.set(0);
                this.queueOffsetMax = 0L;
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }


    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }


    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }


    public Lock getLockConsume() {
        return lockConsume;
    }


    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }


    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }


    public long getMsgAccCnt() {
        return msgAccCnt;
    }


    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }


    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }


    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }


    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
            }

            if (!this.msgTreeMapTemp.isEmpty()) {
                info.setTransactionMsgMinOffset(this.msgTreeMapTemp.firstKey());
                info.setTransactionMsgMaxOffset(this.msgTreeMapTemp.lastKey());
                info.setTransactionMsgCount(this.msgTreeMapTemp.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        }
        catch (Exception e) {
        }
        finally {
            this.lockTreeMap.readLock().unlock();
        }
    }


    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }


    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }
}
