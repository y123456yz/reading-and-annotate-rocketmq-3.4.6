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
package com.alibaba.rocketmq.common.message;

import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;


/**
 * msg信息
 * @author shijia.wxr  http://blog.csdn.net/xxxxxx91116/article/details/50333161 记录了消息存储格式
 * @author shijia.wxr  http://blog.csdn.net/xxxxxx91116/article/details/50333161 记录了消息存储格式
 * 或者参考http://blog.csdn.net/chunlongyu/article/details/54576649
 * 存储中用到的消息内容信息
 */
public class MessageExt extends Message {
    private static final long serialVersionUID = 5720810158625748049L;

    //以下成员几乎在MessageDecoder.decode()中调用赋值

    private int queueId;
    private int storeSize;
    //该条smg对应在队列中的offset 从ProcessQueue.putMessage接口也可以看出
    private long queueOffset; //MessageDecoder.decode()中调用赋值
    private int sysFlag;//标明是否事务处理等
    private long bornTimestamp;
    private SocketAddress bornHost;
    private long storeTimestamp;
    private SocketAddress storeHost;
    private String msgId;
    private long commitLogOffset;
    private int bodyCRC;
    private int reconsumeTimes; //该条消息重复消费的次数，

    private long preparedTransactionOffset;


    public MessageExt() {
    }


    public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp,
            SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static ByteBuffer SocketAddress2ByteBuffer(SocketAddress socketAddress) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    public ByteBuffer getBornHostBytes() {
        return SocketAddress2ByteBuffer(this.bornHost);
    }

    public ByteBuffer getStoreHostBytes() {
        return SocketAddress2ByteBuffer(this.storeHost);
    }


    public int getQueueId() {
        return queueId;
    }


    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }


    public long getBornTimestamp() {
        return bornTimestamp;
    }


    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }


    public SocketAddress getBornHost() {
        return bornHost;
    }


    public String getBornHostString() {
        if (this.bornHost != null) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) this.bornHost;
            return inetSocketAddress.getAddress().getHostAddress();
        }

        return null;
    }


    public String getBornHostNameString() {
        if (this.bornHost != null) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) this.bornHost;
            return inetSocketAddress.getAddress().getHostName();
        }

        return null;
    }


    public void setBornHost(SocketAddress bornHost) {
        this.bornHost = bornHost;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }


    public SocketAddress getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(SocketAddress storeHost) {
        this.storeHost = storeHost;
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public int getSysFlag() {
        return sysFlag;
    }


    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }


    public int getBodyCRC() {
        return bodyCRC;
    }


    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }


    public long getQueueOffset() {
        return queueOffset;
    }

    //MessageDecoder.decode()中调用赋值
    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }


    public long getCommitLogOffset() {
        return commitLogOffset;
    }


    public void setCommitLogOffset(long physicOffset) {
        this.commitLogOffset = physicOffset;
    }


    public int getStoreSize() {
        return storeSize;
    }


    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }


    public static TopicFilterType parseTopicFilterType(final int sysFlag) {
        if ((sysFlag & MessageSysFlag.MultiTagsFlag) == MessageSysFlag.MultiTagsFlag) {
            return TopicFilterType.MULTI_TAG;
        }

        return TopicFilterType.SINGLE_TAG;
    }

    //在sendMessageBack接口使用 DefaultMQPushConsumerImpl.sendMessageBack
    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    //processConsumeResult会调用赋值
    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }


    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }


    public void setPreparedTransactionOffset(long preparedTransactionOffset) {
        this.preparedTransactionOffset = preparedTransactionOffset;
    }


    @Override
    public String toString() {
        return "MessageExt [queueId=" + queueId + ", storeSize=" + storeSize + ", queueOffset=" + queueOffset
                + ", sysFlag=" + sysFlag + ", bornTimestamp=" + bornTimestamp + ", bornHost=" + bornHost
                + ", storeTimestamp=" + storeTimestamp + ", storeHost=" + storeHost + ", msgId=" + msgId
                + ", commitLogOffset=" + commitLogOffset + ", bodyCRC=" + bodyCRC + ", reconsumeTimes="
                + reconsumeTimes + ", preparedTransactionOffset=" + preparedTransactionOffset
                + ", toString()=" + super.toString() + "]";
    }
}
