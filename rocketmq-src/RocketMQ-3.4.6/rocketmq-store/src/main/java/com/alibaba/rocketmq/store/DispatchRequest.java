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

/**
 *
 *
 *  ConsumeQueue 创建过程：
 *  1. 首先会创建CommitLog，在将数据写入CommitLog之后，调用defaultMessageStore->doReput->doDispatch
 *  2. doDispatch 调用 putMessagePostionInfo 将数据写入ConsumeQueue
 *
 *  IndexService用于创建索引文件集合，当用户想要查询某个topic下某个key的消息时，能够快速响应；
 *  这里注意不要与上述的ConsumeQueue混合，ConsumeQueue只是为了抽象出多个queue，方便并发情况下，用户put/get消息。
 *  IndexFile的创建过程：
 *  1. 首先在 doDispatch 写入ConsumeQueue后，会再调用indexService.putRequest，创建索引请求
 *  2. 调用IndexService的buildIndex创建索引
 *

// 异步线程分发 commitlog 文件中的消息到 consumeQueue 或者分发到 indexService 见 ReputMessageService
// 1.分发消息位置到 ConsumeQueue
// 2.分发到 IndexService 建立索引
 *
 *  checkMessageAndReturnSize中构建DispatchRequest
 * @author shijia.wxr
 */
public class DispatchRequest {
    private final String topic;
    private final int queueId; //commitlog中存储的消息格式已经指定好该消息对应的topic，已经存到consumeQueue中对应的topic的那个队列

    /*
    *
    * CommitLog Offset是指这条消息在Commit Log文件中的实际偏移量
    * Size存储中消息的大小
    * Message Tag HashCode存储消息的Tag的哈希值：主要用于订阅时消息过滤（订阅时如果指定了Tag，会根据HashCode来快速查找到订阅的消息）
    * */
    //consumequeue文件存储单元格式:CommitLog Offset + Size +Message Tag HashCode
    private final long commitLogOffset; //consumequeue文件存储单元格式:CommitLog Offset + Size +Message Tag HashCode中的offset
    private final int msgSize;//consumequeue文件存储单元格式:CommitLog Offset + Size +Message Tag HashCode中的size
    //tagsString2tagsCode  发送消息的时候带的TAG做HASH
    private final long tagsCode;//consumequeue文件存储单元格式:CommitLog Offset + Size +Message Tag HashCode中的Message Tag HashCode

    private final long storeTimestamp;
    private final long consumeQueueOffset;
    private final String keys;
    private final boolean success;
    private final int sysFlag;
    private final long preparedTransactionOffset;

    //checkMessageAndReturnSize 中new一个该类
    public DispatchRequest(//
            final String topic,// 1
            final int queueId,// 2
            final long commitLogOffset,// 3
            final int msgSize,// 4
            final long tagsCode,// 5
            final long storeTimestamp,// 6
            final long consumeQueueOffset,// 7
            final String keys,// 8
            final int sysFlag,// 9
            final long preparedTransactionOffset// 10
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
    }


    public DispatchRequest(int size) {
        // 1
        this.topic = "";
        // 2
        this.queueId = 0;
        // 3
        this.commitLogOffset = 0;
        // 4
        this.msgSize = size;
        // 5
        this.tagsCode = 0;
        // 6
        this.storeTimestamp = 0;
        // 7
        this.consumeQueueOffset = 0;
        // 8
        this.keys = "";

        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
    }


    public DispatchRequest(int size, boolean success) {
        // 1
        this.topic = "";
        // 2
        this.queueId = 0;
        // 3
        this.commitLogOffset = 0;
        // 4
        this.msgSize = size;
        // 5
        this.tagsCode = 0;
        // 6
        this.storeTimestamp = 0;
        // 7
        this.consumeQueueOffset = 0;
        // 8
        this.keys = "";

        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
    }


    public String getTopic() {
        return topic;
    }


    public int getQueueId() {
        return queueId;
    }


    public long getCommitLogOffset() {
        return commitLogOffset;
    }


    public int getMsgSize() {
        return msgSize;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }


    public String getKeys() {
        return keys;
    }


    public long getTagsCode() {
        return tagsCode;
    }


    public int getSysFlag() {
        return sysFlag;
    }


    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }


    public boolean isSuccess() {
        return success;
    }
}
