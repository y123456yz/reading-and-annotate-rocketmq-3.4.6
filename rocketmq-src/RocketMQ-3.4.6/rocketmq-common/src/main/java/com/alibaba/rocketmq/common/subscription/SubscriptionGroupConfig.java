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

package com.alibaba.rocketmq.common.subscription;

import com.alibaba.rocketmq.common.MixAll;


/**
 * 某一个消费者分组的订阅配置。
 *， 重试队列 重试消费都在这里
 * @author shijia.wxr
 */
public class SubscriptionGroupConfig {
    private String groupName;
    private boolean consumeEnable = true;
    private boolean consumeFromMinEnable = true;
    private boolean consumeBroadcastEnable = true;
    //某个消费分组xxxx对应的重试队列，如果一个消费则分组消费多个topic，其重试队列也只有一个，即%RETRY%xxxx，表示该消费者分组消费失败的消息全部存到该重试队列中
    private int retryQueueNums = 1; //通过broker.properties 中的retryMaxTimes进行配置，如果配置为0，表示不允许有重试队列，消费失败打过来后，直接返回OK
    private int retryMaxTimes = 16; //消费失败重试消费最大次数 broker.properties参数可配置
    private long brokerId = MixAll.MASTER_ID;
    private long whichBrokerWhenConsumeSlowly = 1;

    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }


    public boolean isConsumeEnable() {
        return consumeEnable;
    }


    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }


    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }


    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }


    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }


    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
    }


    public int getRetryQueueNums() {
        return retryQueueNums;
    }


    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }


    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }


    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }


    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }


    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result =
                prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        if (brokerId != other.brokerId)
            return false;
        if (consumeBroadcastEnable != other.consumeBroadcastEnable)
            return false;
        if (consumeEnable != other.consumeEnable)
            return false;
        if (consumeFromMinEnable != other.consumeFromMinEnable)
            return false;
        if (groupName == null) {
            if (other.groupName != null)
                return false;
        }
        else if (!groupName.equals(other.groupName))
            return false;
        if (retryMaxTimes != other.retryMaxTimes)
            return false;
        if (retryQueueNums != other.retryQueueNums)
            return false;
        if (whichBrokerWhenConsumeSlowly != other.whichBrokerWhenConsumeSlowly)
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
                + ", consumeFromMinEnable=" + consumeFromMinEnable + ", consumeBroadcastEnable="
                + consumeBroadcastEnable + ", retryQueueNums=" + retryQueueNums + ", retryMaxTimes="
                + retryMaxTimes + ", brokerId=" + brokerId + ", whichBrokerWhenConsumeSlowly="
                + whichBrokerWhenConsumeSlowly + "]";
    }
}
