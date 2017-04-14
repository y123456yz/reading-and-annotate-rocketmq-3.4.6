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

import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * @author shijia.wxr   PullResultExt类继承该类
 */
public class PullResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    //客户端拉取到消息后在通过 PullAPIWrapper.processPullResult 中把接收到的消息赋值给msgFoundList
    //拉取到的消息，匹配tag后存入msgFoundList   赋值见PullAPIWrapper.processPullResult
    //拉取到消息后首先存入PullResult.msgFoundList，在DefaultMQPushConsumerImpl.pullMessage然后进行规则匹配，匹配的msg会进一步存入ProcessQueue.msgTreeMap
    private List<MessageExt> msgFoundList;

    //MQClientAPIImpl.processPullResponse中调用该接口，并赋值相关成员变量
    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                      List<MessageExt> msgFoundList) {
        super();
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.msgFoundList = msgFoundList;
    }


    public PullStatus getPullStatus() {
        return pullStatus;
    }


    public long getNextBeginOffset() {
        return nextBeginOffset;
    }


    public long getMinOffset() {
        return minOffset;
    }


    public long getMaxOffset() {
        return maxOffset;
    }


    public List<MessageExt> getMsgFoundList() {
        return msgFoundList;
    }

    //客户端拉取到消息后在通过 PullAPIWrapper.processPullResult 中把接收到的消息赋值给msgFoundList
    //然后在 DefaultMQPushConsumerImpl.pullMessage 中取出消息进行 GroovyScript 匹配
    public void setMsgFoundList(List<MessageExt> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }


    @Override
    public String toString() {
        return "PullResult [pullStatus=" + pullStatus + ", nextBeginOffset=" + nextBeginOffset
                + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", msgFoundList="
                + (msgFoundList == null ? 0 : msgFoundList.size()) + "]";
    }
}
