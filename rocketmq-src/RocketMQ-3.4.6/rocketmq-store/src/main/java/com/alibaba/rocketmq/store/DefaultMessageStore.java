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
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.*;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.running.RunningStats;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;
import com.alibaba.rocketmq.store.ha.HAService;
import com.alibaba.rocketmq.store.index.IndexService;
import com.alibaba.rocketmq.store.index.QueryOffsetResult;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.rocketmq.store.config.BrokerRole.SLAVE;


/**
 *  无论是commitlogy ，还是consume queue，都是以mapfile作为文件操作的载体。
 *
 *  commitlog 具有全局唯一的Offset.(64位） ；
 *
 *
 *
 *
 * @author shijia.wxr
 */
public class DefaultMessageStore implements MessageStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private final MessageFilter messageFilter = new DefaultMessageFilter();
    private final MessageStoreConfig messageStoreConfig;
    private final CommitLog commitLog;
    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;
    private final FlushConsumeQueueService flushConsumeQueueService;
    private final CleanCommitLogService cleanCommitLogService;
    private final CleanConsumeQueueService cleanConsumeQueueService;
    private final IndexService indexService;
    private final AllocateMapedFileService allocateMapedFileService;
    private final ReputMessageService reputMessageService;
    private final HAService haService;
    //如果不为NULL，则说明启用了延迟消息服务。  延迟消息
    private final ScheduleMessageService scheduleMessageService;
    private final StoreStatsService storeStatsService;
    private final RunningFlags runningFlags = new RunningFlags(); //是否可读 可写
    private final SystemClock systemClock = new SystemClock(1);
    private volatile boolean shutdown = true;
    private StoreCheckpoint storeCheckpoint;
    private AtomicLong printTimes = new AtomicLong(0);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;


    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
            final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMapedFileService = new AllocateMapedFileService(this);
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        this.haService = new HAService(this);

        //异步构建CQ和索引文件的服务。
        this.reputMessageService = new ReputMessageService();
        //延迟消息投递服务
        this.scheduleMessageService = new ScheduleMessageService(this);

        this.allocateMapedFileService.start();
        this.indexService.start();
    }


    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    //执行见 BrokerController.initialize    delayOffset.json  consumerOffset.json  commitlog  consumequeue 加载到内存
    public boolean load() {
        boolean result = true;

        try {
            //临时文件不存在 ，说明 broker没有异常退出。
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", (lastExitOK ? "normally" : "abnormally"));
            if (null != scheduleMessageService) { //延迟消息服务,定时进度。
                result = result && this.scheduleMessageService.load(); //加载delayOffset.json
            }

            /*  /data/store中的commitlog   consumequeue 加载 */
            result = result && this.commitLog.load();
            result = result && this.loadConsumeQueue(); //加载consumequeue

            if (result) {
                //加载存储检查点
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                //加载索引服务indexService
                this.indexService.load(lastExitOK);

                /*
                *  recover尝试数据恢复
                判断是否是正常恢复，系统启动的启动存储服务(DefaultMessageStore)的时候会创建一个临时文件abort, 当系统正常关闭的
                时候会把这个文件删掉，这个类似在Linux下打开vi编辑器生成那个临时文件，所有当这个abort文件存在，系统认为是异常恢复
                * */
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        }
        catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMapedFileService.shutdown();
        }

        return result;
    }


    private void addScheduleTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);
    }


    private void cleanFilesPeriodically() {
        //先按过期时间清理掉commitlog ,
        this.cleanCommitLogService.run();
        //然后以commitlog清理完毕以后 mapfile的最小物理位点来清理消费队列。 (也就是说consume queue中比commitlog最小物理位点还要小的item都删除掉） 。
        this.cleanConsumeQueueService.run();
    }


    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }


    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",//
                            nextQT.getValue().getTopic(),//
                            nextQT.getValue().getQueueId(),//
                            nextQT.getValue().getMaxPhysicOffset(),//
                            nextQT.getValue().getMinLogicOffset());
                    }
                    else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",//
                            topic,//
                            nextQT.getKey(),//
                            minCommitLogOffset,//
                            maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueurFromTopicQueueTable(nextQT.getValue().getTopic(), nextQT.getValue()
                            .getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }


    public void start() throws Exception {
        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        if (this.scheduleMessageService != null && SLAVE != messageStoreConfig.getBrokerRole()) {
            this.scheduleMessageService.start();
        }

        this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        this.reputMessageService.start();

        this.haService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {
                Thread.sleep(1000 * 3);
            }
            catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            this.haService.shutdown();

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.allocateMapedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable()) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
            }
            else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }
    }


    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }


    public void destroyLogics() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }


    /**
     *
     * @param msg   msg写入commitlog文件
     * @return
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) { //不能写消息到slave
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) { //该broker不可写
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        else {
            this.printTimes.set(0);
        }

        //topic长度超过了127，禁止 。
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        //消息的属性字符串超过了 32767
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessage(msg); //写入commitlog文件
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 1000) {
            log.warn("putMessage not in lock eclipse time(ms) " + eclipseTime);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime); //统计时延分布

        if (null == result || !result.isOk()) { //写入消息失败次数累加。
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }


    public SystemClock getSystemClock() {
        return systemClock;
    }


    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }




    /**
     * 消费者分组从 的topic的某一个consume queue的指定位点（消费队列的逻辑位点） 开始拉消息，一次最多maxMsgNums 条，并且用指定的
     * 订阅表达式进行过滤。
     * @param group
     * @param topic
     * @param queueId
     * @param offset 消费队列的 逻辑位点。
     * @param maxMsgNums
     * @param subscriptionData
     * @return
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums,
            final SubscriptionData subscriptionData) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        //下一次从消费队列的哪个位点拉 。
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) { //cq中的最小和最大位点(逻辑 )
            minOffset = consumeQueue.getMinOffsetInQuque();
            maxOffset = consumeQueue.getMaxOffsetInQuque();

            if (maxOffset == 0) { //最大位点为0  ， 下次位点从0开始。
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            }
            else if (offset < minOffset) { //请求位点小于最小位点， 下次位点从最小位点开始。
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            }
            else if (offset == maxOffset) { //请求位点等于最大位点，
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            }
            else if (offset > maxOffset) { //请求位点超过最大位点。
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {  //从最小位点0开始 。
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                }
                else { //从maxoffset开始。
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            }
            else {
                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) { //一次从消费队列中拉取出了多个索引项。
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int MaxFilterMessageCount = 16000;
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        for (; i < bufferConsumeQueue.getSize() && i < MaxFilterMessageCount; i += ConsumeQueue.CQStoreUnitSize) {
                            //一次最多过滤16000条消息, 或者一次把消费队列中的数据拉完。
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong(); // commitlog 位点。
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt(); //commitlog中消息的长度
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong(); // 消息tags的hashcode .

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                    isInDisk)) { //一旦拉满了，则退出for循环不拉了。
                                break;
                            }

                            //先对Message tag的hashcode做第一次过滤，
                            if (this.messageFilter.isMessageMatched(subscriptionData, tagsCode)) {
                                SelectMapedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                                if (selectResult != null) {
                                    //2016.11.15 增加groovy匹配脚本的二次过滤, 对吞吐量会有一定影响,
                                    //吞吐量下降以后，client 拉取消息的异步请求被定时任务判定为超时而删除掉 。所以这里把消息的过滤放在client
                                    //做更加合理。
                                    /*
                                    if(subscriptionData.getScript() != null){ //有匹配脚本时，只有匹配上，才把消息加入消息列表
                                        if(this.isMsgMatchGroopyScript(
                                                selectResult.getByteBuffer(),subscriptionData.getScript(), subscriptionData.getGroovyScript())){
                                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                                            getResult.addMessage(selectResult); //消息匹配，加入消息列表 。
                                            status = GetMessageStatus.FOUND;
                                            nextPhyFileStartOffset = Long.MIN_VALUE;
                                        }else{ //消息没有匹配上匹配脚本
                                            if (getResult.getBufferTotalSize() == 0) {
                                                status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                            }

                                            if (log.isDebugEnabled()) {
                                                log.debug("message type not matched, client: " + subscriptionData + " server: " + tagsCode);
                                            }
                                        }
                                    }else{ //没有匹配脚本， 则把消息加入匹配消息列表。
                                        this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                                        getResult.addMessage(selectResult); //消息匹配，加入消息列表 。
                                        status = GetMessageStatus.FOUND;
                                        nextPhyFileStartOffset = Long.MIN_VALUE;
                                    }
                                    */

                                    this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                                    getResult.addMessage(selectResult); //消息匹配，加入消息列表 。
                                    status = GetMessageStatus.FOUND;
                                    nextPhyFileStartOffset = Long.MIN_VALUE;
                                }
                                else {
                                    if (getResult.getBufferTotalSize() == 0) {
                                        status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                    }

                                    nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                }
                            }
                            else { //没有匹配消息， 处理下一批  。
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                if (log.isDebugEnabled()) {
                                    log.debug("message type not matched, client: " + subscriptionData + " server: " + tagsCode);
                                }
                            }
                        }

                        if (diskFallRecorded) { //记录commitlog的物理位点和当前拉取到的位点的差异。
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehind(group, topic, queueId, fallBehind);
                        }
                        //拉完这一批以后，下次从队列的哪个位点开始。
                        nextBeginOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);

                        //如果写入commitlog的物理位点和读取的物理位点差值(理解为消息堆积量) 比预设的最大的内存容量要大 ， 则推荐client从slave拉。
                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory =
                                (long) (StoreUtil.TotalPhysicalMemorySize * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    }
                    finally {
                        bufferConsumeQueue.release();
                    }
                }
                else {
                    //在当前这个mapfile没有找到CQ item ,则前移一个mapfile .
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed.");
                }
            }
        }
        else { //找不要匹配的消费队列。
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        }
        else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset); //下一次CQ的逻辑位点。
        getResult.setMaxOffset(maxOffset); //CQ的最大逻辑位点。
        getResult.setMinOffset(minOffset); //CQ的最小逻辑位点。
        return getResult;
    }

    public long getMaxOffsetInQuque(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQuque();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQuque(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQuque();
        }

        return -1;
    }


    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    //http://blog.csdn.net/xxxxxx91116/article/details/50333161
    //通过commitLogOffset位点从对应的MapedFile中找到对应的消息，commitlog文件每条消息存储格式参考:http://blog.csdn.net/xxxxxx91116/article/details/50333161
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE  该消息的总长度
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }


    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }


    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();
        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }


    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }


    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMapedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQStoreUnitSize);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getCommitLog().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                }
                catch (Exception e) {
                }
                finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long offset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMapedBufferResult result = logicQueue.getIndexBuffer(offset);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getCommitLog().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                }
                catch (Exception e) {
                }
                finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }


    @Override
    public SelectMapedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }


    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        }
        else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }


    @Override
    public void excuteDeleteFilesManualy() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }


    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {
                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
                    if (topic.equals(msg.getTopic())) {
                        for (String k : keyArray) {
                            if (k.equals(key)) {
                                match = true;
                                break;
                            }
                        }
                    }

                    if (match) {
                        SelectMapedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    }
                    else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                }
                catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }


    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }


    @Override
    public long now() {
        return this.systemClock.now();
    }


    public CommitLog getCommitLog() {
        return commitLog;
    }

    //解析commitlog中位点从commitLogOffset开始的size字节，解析出来存入MessageExt，并返回
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMapedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentHashMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            }
            else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(//
                topic,//
                queueId,//
                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),//
                this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),//
                this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            }
            else {
                logic = newLogic;
            }
        }

        return logic;
    }


    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        //从消息的总量来判断批处理操作 满了。
        if ((messageTotal + 1) >= maxMsgNums) {
            return true;
        }


        if (isInDisk) { //需要读盘， 并且消息的大小维度或者消息的个数维度超过了限制。
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk()) {
                return true;
            }
        }
        else { //需要读内存， 并且消息的大小维度或者消息的个数维度超过了限制。
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if ((messageTotal + 1) > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory()) {
                return true;
            }
        }

        return false;
    }


    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MapedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    //判断路径是否存在  /root/store
    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }


    /**
     *
     [root@s10-2-s-5 topic-prod-xxxxxservice-xxxxx]# pwd
     /data/store/consumequeue/topic-prod-xxxxxservice-xxxxx
     [root@s10-2-s-5 topic-prod-xxxxxservice-xxxxx]# ls
     0  1  2  3  4  5  6  7  8  9
     [root@s10-2-s-5 topic-prod-xxxxxservice-xxxxx]# ls 0/
     00000000000048000000  00000000000054000000  00000000000060000000  00000000000066000000  00000000000072000000  00000000000078000000  00000000000084000000  00000000000090000000  00000000000096000000  00000000000102000000
     [root@s10-2-s-5 topic-prod-xxxxxservice-xxxxx]#
     * 消费队列的目录结构
     * /topic/queueid/mapfile 其中mapfile的文件名是？
     * Consume Queue存储消息在Commit Log中的位置信息
     * @return
     */
    // DefaultMessageStore.loadConsumeQueue broker起来后，从/data/store/consumequeue路径读取对应topic中各个队列的commit log索引信息
    private boolean loadConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId = Integer.parseInt(fileQueueId.getName());
                        ConsumeQueue logic = new ConsumeQueue(//
                            topic,//
                            queueId,//
                            StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),//
                            this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),//
                            this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentHashMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        }
        else {
            map.put(queueId, consumeQueue);
        }
    }


    private void recover(final boolean lastExitOK) {
        this.recoverConsumeQueue();

        if (lastExitOK) {
            this.commitLog.recoverNormally();
        }
        else {
            this.commitLog.recoverAbnormally();
        }

        this.recoverTopicQueueTable();
    }


    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQuque());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }


    private void recoverConsumeQueue() {
        for (ConcurrentHashMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
            }
        }
    }


    /**
     * 在消费队列(CQ)中增加索引项。
     * @param topic  消息主题
     * @param queueId 消费队列id .
     * @param offset commitlog物理位点。
     * @param size  commitlog中消息的大小。
     * @param tagsCode  tag的hashcode.
     * @param storeTimestamp 日志写入时间，
     * @param logicOffset 消费队列（CQ）的逻辑位点。
     */
    public void putMessagePostionInfo(String topic, int queueId, long offset, int size, long tagsCode, long storeTimestamp, long logicOffset) {
        ConsumeQueue cq = this.findConsumeQueue(topic, queueId); //根据commitlog中该条消息的topic和queueID找到该条消息对应的ConsumeQueue
        cq.putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset);
    }


    public AllocateMapedFileService getAllocateMapedFileService() {
        return allocateMapedFileService;
    }


    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }


    public RunningFlags getAccessRights() {
        return runningFlags;
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }


    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public HAService getHaService() {
        return haService;
    }


    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }


    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    class CleanCommitLogService {
        private final static int MaxManualDeleteFileTimes = 20;
        private final double DiskSpaceWarningLevelRatio = Double.parseDouble(System.getProperty(
            "rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));
        private final double DiskSpaceCleanForciblyRatio = Double.parseDouble(System.getProperty(
            "rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;
        private volatile int manualDeleteFileSeveralTimes = 0;
        private volatile boolean cleanImmediately = false;


        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MaxManualDeleteFileTimes;
            DefaultMessageStore.log.info("excuteDeleteFilesManualy was invoked");
        }


        public void run() {
            try {
                this.deleteExpiredFiles();

                this.redeleteHangedFile();
            }
            catch (Exception e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }


        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                    // TODO
                }
            }
        }


        private void deleteExpiredFiles() {
            int deleteCount = 0;
            //默认48小时。
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            //两个Mapfile文件清理的时间间隔，默认100ms. ;
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            //当Mapfile还在被线程引用无法删除时， 下一次再进行强制清理的时间间隔， 默认120s
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            boolean timeup = this.isTimeToDelete(); //到了清理commitlog的时间，默认凌晨4点。
            boolean spacefull = this.isSpaceToDelete(); //commitlog或者consume queue的目录利用率超过75% ，启动文件清理。
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0; //手工删除日志文件的最大次数默认20 。

            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",//
                        fileReservedTime,//
                        timeup,//
                        spacefull,//
                        manualDeleteFileSeveralTimes,//
                        cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount =
                        DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                                destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                    // TODO
                }
                else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }


        /**
         *  commitlog和消费队列的利用率超过75%， 则需要做文件清理；
         *  超过85% ,需要做文件清理。
         *  超过90% ，需要立即做文件清理， 同时打开运行标识位中的diskfull标记，并触发一次显式gc.
         * @return
         */
        private boolean isSpaceToDelete() {
            //磁盘空间利用率最大为75%
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (physicRatio > DiskSpaceWarningLevelRatio) { //commitlog的目录利用率超过90%, 立即清理。
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                        System.gc(); //通过显式gc触发堆外内存回收？

                    }

                    cleanImmediately = true;
                }
                else if (physicRatio > DiskSpaceCleanForciblyRatio) { //commitlog目录的利用率超过85%(立即清理)
                    cleanImmediately = true;
                }
                else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) { //超过75% ，也立即清理。
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            {
                String storePathLogics =
                        StorePathConfigHelper.getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig()
                            .getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > DiskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                        System.gc();
                    }

                    cleanImmediately = true;
                }
                else if (logicsRatio > DiskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                }
                else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }


        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }


        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;


        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            }
                            catch (InterruptedException e) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }


        public void run() {
            try {
                this.deleteExpiredFiles();
            }
            catch (Exception e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;


        private void doFlush(int retryTimes) {

            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RetryTimesOver) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentHashMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.commit(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }


        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                }
                catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RetryTimesOver);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }


    /**
     * 构建消费队列项和msgkey的索引项。
     * @param req
     */
    public void doDispatch(DispatchRequest req) {
        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
        switch (tranType) {
        case MessageSysFlag.TransactionNotType:
        case MessageSysFlag.TransactionCommitType:
            //把从commitlog中读取到的消息与consumeQueue关联起来，把该条消息的offset len taghash写入到 consumeQueue
            DefaultMessageStore.this.putMessagePostionInfo(req.getTopic(), req.getQueueId(), req.getCommitLogOffset(), req.getMsgSize(),
                req.getTagsCode(), req.getStoreTimestamp(), req.getConsumeQueueOffset());
            break;
        case MessageSysFlag.TransactionPreparedType:
        case MessageSysFlag.TransactionRollbackType:
            break;
        }

        if (DefaultMessageStore.this.getMessageStoreConfig().isMessageIndexEnable()) {
            DefaultMessageStore.this.indexService.buildIndex(req);
        }
    }

    /*
    *  ConsumeQueue创建过程：
    *  1. 首先会创建CommitLog，在将数据写入CommitLog之后，调用defaultMessageStore->doReput->doDispatch
    *  2. doDispatch 调用 putMessagePostionInfo 将数据写入ConsumeQueue
    *
    *  IndexService用于创建索引文件集合，当用户想要查询某个topic下某个key的消息时，能够快速响应；
    *  这里注意不要与上述的ConsumeQueue混合，ConsumeQueue只是为了抽象出多个queue，方便并发情况下，用户put/get消息。
    *  IndexFile的创建过程：
    *  1. 首先在 doDispatch 写入ConsumeQueue后，会再调用indexService.putRequest，创建索引请求
    *  2. 调用IndexService的buildIndex创建索引
    * */

    // 异步线程分发 commitlog 文件中的消息到 consumeQueue 或者分发到 indexService 见 ReputMessageService
    // 1.分发消息位置到 ConsumeQueue
    // 2.分发到 IndexService 建立索引
    class ReputMessageService extends ServiceThread { //run执行触发见 DefaultMessageStore
        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                    DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        private volatile long reputFromOffset = 0;


        public long getReputFromOffset() {
            return reputFromOffset;
        }


        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }


        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }


        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }


        private void doReput() {
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext;) {
                //reputFromOffset是 commitlog 的最大物理位点，现在写入到这个位置了。
                //通过commitlogoffset从commitlog文件中获取该消息内容信息
                SelectMapedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext;) { //从commitlog中获取当前已经写入的一批消息来构建消费队列和索引文件。
                            //根据消息内容构建 DispatchRequest 类
                            DispatchRequest dispatchRequest = //从commitlog中读取和构建CQ和索引文件相关的内容。
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getMsgSize();
                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    DefaultMessageStore.this.doDispatch(dispatchRequest); //构建索引项
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                        //Master broker打开了对长轮询的支持 . 通知NotifyMessageArrivingListener 有新的消息到来。
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1);
                                    }
                                    // bugfix By shijia
                                    // 下一次从该条消息的下一个Offset处dispatch消息到consumequeue 和 IndexService
                                    this.reputFromOffset += size; //下一次commitlog的位点从哪儿开始。

                                    readSize += size;
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicTimesTotal(
                                                dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicSizeTotal(
                                                dispatchRequest.getTopic()).addAndGet(dispatchRequest.getMsgSize());
                                    }
                                }
                                else if (size == 0) { //读到的消息长度为0 ， 切换到下一个 文件。
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            }
                            else if (!dispatchRequest.isSuccess()) {
                                if (size > 0) { //从commitlog读取的消息有问题，继续下一个消息。
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                }
                                else { //从commitlog已经读不到消息。
                                    doNext = false;
                                    if (DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]the master dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);
                                        //位点重新回溯，重建消费队列和索引文件?
                                        this.reputFromOffset += (result.getSize() - readSize);
                                    }
                                }
                            }
                        }
                    }
                    finally {
                        result.release();
                    }
                }
                else { //没有新的数据能从commitlog读取出来 。
                    doNext = false;
                }
            }
        }


        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    //每间隔1Ms 做一次 CQ和索引文件的写入。
                    Thread.sleep(1);
                    this.doReput();
                }
                catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }


    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long cqOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(cqOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                }
                finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }


    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }


    /**
     * 获取commitlog的最大commit位点和ha服务推送给slave的最大位点的差异。
     * @return
     */
    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }


    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",//
                        cq.getTopic(), //
                        cq.getQueueId() //
                    );

                    this.commitLog.removeQueurFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }


    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset, SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQuque());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQuque());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.SocketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    }
                    finally {
                        bufferConsumeQueue.release();
                    }
                }
                else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }


    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TotalPhysicalMemorySize * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }


    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMapedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize();) {
                        i += ConsumeQueue.CQStoreUnitSize;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                }
                finally {
                    bufferConsumeQueue.release();
                }
            }
            else {
                return false;
            }
        }
        return false;
    }


    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }


    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }
}
