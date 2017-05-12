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

/**
 * $Id: CreateTopicRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr
 */
public class CreateTopicRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String defaultTopic;
    /*
    * consumequeue/topic/目录下的数字编号，这些编号由写队列数决定。
例如创建队列的时候设置读队列数为2，写队列为10，那么消息的索引信息会记录到consumequeue/topic/下面的0-9数字编号文件中，
但是因为读队列数为2，那么也就只能消费0和1索引队列指定的消息可以被消费者消费，索引2-9队列上的消息就不能消费
    * */
    @CFNotNull
    private Integer readQueueNums;
    @CFNotNull
    private Integer writeQueueNums;
    @CFNotNull
    private Integer perm;
    @CFNotNull
    private String topicFilterType;
    private Integer topicSysFlag;
    @CFNotNull
    private Boolean order = false;


    @Override
    public void checkFields() throws RemotingCommandException {
        try {
            TopicFilterType.valueOf(this.topicFilterType);
        }
        catch (Exception e) {
            throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
        }
    }


    public TopicFilterType getTopicFilterTypeEnum() {
        return TopicFilterType.valueOf(this.topicFilterType);
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getDefaultTopic() {
        return defaultTopic;
    }


    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    public Integer getReadQueueNums() {
        return readQueueNums;
    }


    public void setReadQueueNums(Integer readQueueNums) {
        this.readQueueNums = readQueueNums;
    }


    public Integer getWriteQueueNums() {
        return writeQueueNums;
    }


    public void setWriteQueueNums(Integer writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }


    public Integer getPerm() {
        return perm;
    }


    public void setPerm(Integer perm) {
        this.perm = perm;
    }


    public String getTopicFilterType() {
        return topicFilterType;
    }


    public void setTopicFilterType(String topicFilterType) {
        this.topicFilterType = topicFilterType;
    }


    public Integer getTopicSysFlag() {
        return topicSysFlag;
    }


    public void setTopicSysFlag(Integer topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }


    public Boolean getOrder() {
        return order;
    }


    public void setOrder(Boolean order) {
        this.order = order;
    }
}
