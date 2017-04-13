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
package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/*
投递消息到broker，tcpdump抓包
* ...5...%{"code":310,"extFields":{"f":"0","g":"1491978653614","d":"4","e":"0","b":"yyztest2","c":"TBW102","a":"yyzGroup2",
* "j":"0","k":"false","h":"0","i":"TAGS\u0001TAG\u0002WAIT\u0001true\u0002KEYS\u0001ffff\u0002"},"flag":0,"language":"JAVA",
* "opaque":3,"serializeTypeCurrentRPC":"JSON","version":115}yang ya zhou
*
* broker收到后，发送如下应答：
* ........{"code":0,"extFields":{"queueId":"0","msgId":"0A02DFA500002A9F0000000163E60ADB","queueOffset":"37"},"flag":1,
* "language":"JAVA","opaque":3,"serializeTypeCurrentRPC":"JSON","version":115}
* */
public class PushConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("yyzGroup2");
        consumer.setNamesrvAddr("10.2.223.157:9876;10.2.223.158:9876;10.2.223.159:9876");
       // consumer.setNamesrvAddr("10.2.223.228:9876");
        // groovyScript = "import groovy.json.JsonSlurper \n def fullTradenfo = new JsonSlurper().parseText(msgJsonContent) \n def fullOrderInfo = fullTradeInfo.orders[0] \n return fullOrderInfo.boolFightGroup||fullOrderInfo.boolFightGroupPresell";

        //consumer.subscribe("my-topic-2", "*", new GroovyScript(groovyScript));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.subscribe("yyztest2", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override  //consumeMessage在ConsumeMessageConcurrentlyService中的接口consumeMessageDirectly中執行該函數
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                if(msgs == null || msgs.size() == 0){
                    System.out.println("not get msgs"); /* Add Trace Log */
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                System.out.println("consumeMessage recv {} Msgs:"+msgs.size());
                for (MessageExt msg : msgs) { /* Add Trace Log */
                    System.out.println("Msgid:{}" +msg.getMsgId());
                }

                for(MessageExt messageExt: msgs) {
                    System.out.println("recv msg with topic:" + messageExt.getTopic() + ",msgTag:" + messageExt.getTags() +  ", body:" + new String(messageExt.getBody()));
                }
                 /* 如果返回不是成功，则该msgs消息会在内存中，offset还是在上次的位置 */
                //业务处理消息后，对返回值的检查在ConsumeRequest.run-> ConsumeMessageConcurrentlyService.processConsumeResult 中
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
