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

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;


/*
投递消息到broker，tcpdump抓包
* ...5...%{"code":310,"extFields":{"f":"0","g":"1491978653614","d":"4","e":"0","b":"yyztest2","c":"TBW102","a":"yyzGroup2",
* "j":"0","k":"false","h":"0","i":"TAGS\u0001TAG\u0002WAIT\u0001true\u0002KEYS\u0001ffff\u0002"},"flag":0,"language":"JAVA",
* "opaque":3,"serializeTypeCurrentRPC":"JSON","version":115}yang ya zhou
*
* broker收到后，发送如下应答：
* ........{"code":0,"extFields":{"queueId":"0","msgId":"0A02DFA500002A9F0000000163E60ADB","queueOffset":"37"},"flag":1,
* "language":"JAVA","opaque":3,"serializeTypeCurrentRPC":"JSON","version":115}
*
* 如果是生产者，断开连接后会注销producer，抓包如下:
*客户端向broker发送注销 yyzGroup2 消费分组请求
* ........{"code":35,"extFields":{"clientID":"192.168.56.1@7644","producerGroup":"yyzGroup2"},"flag":0,
* "language":"JAVA","opaque":12,"serializeTypeCurrentRPC":"JSON","version":115}
* broker会有注销成功
* ...s...o{"code":0,"extFields":{},"flag":1,"language":"JAVA","opaque":12,"serializeTypeCurrentRPC":"JSON","version":115}
*
*客户端向broker发送注销 CLIENT_INNER_PRODUCER 消费分组请求
* ........{"code":35,"extFields":{"clientID":"192.168.56.1@7644","producerGroup":"CLIENT_INNER_PRODUCER"},"flag":0,"
* language":"JAVA","opaque":20,"serializeTypeCurrentRPC":"JSON","version":115}
*
* broker会有注销成功
* ...s...o{"code":0,"extFields":{},"flag":1,"language":"JAVA","opaque":20,"serializeTypeCurrentRPC":"JSON","version":115}
* */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("yyzGroup2");
        producer.setNamesrvAddr("10.2.223.157:9876;10.2.223.158:9876;10.2.223.159:9876");
       // producer.setNamesrvAddr("10.2.223.228:9876");
        producer.start();
        for(int i = 0; i < 1; ++i) {
//            Thread.currentThread().sleep(50);
//            for (String item : array) {
            Message msg = new Message("yyztest2",// topic
                    "TAG",// tag
                    "ffff",// 注意， msgkey对帮助业务排查消息投递问题很有帮助，请设置成和消息有关的业务属性，比如订单id ,商品id .
                    "yang ya zhou".getBytes());// body //默认会设置等待消息存储成功。
            SendResult sendResult = null;
            try {//同步发送消息 ，并且等待消息存储成功，超时时间3s .
                System.out.println("send msg with msgKey:" + msg.getKeys());
                sendResult = producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(sendResult);
        }
        System.out.println(System.getProperty("user.home") );
        producer.shutdown();
    }
}
