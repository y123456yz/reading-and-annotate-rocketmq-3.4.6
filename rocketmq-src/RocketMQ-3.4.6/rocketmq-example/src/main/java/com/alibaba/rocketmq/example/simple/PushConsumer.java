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
    *
1.PullMessageService.run注册 PullRequest 来向broker拉取消息
上面的 PullMessageService.run 拉取到消息后，继续以下步骤:
1. DefaultMQPushConsumerImpl.pullMessage中注册异步回调接口 pullCallback.onSuccess
2.pullMessageAsync->pullCallback.onSuccess  netty异步获取消息，走到这里开始执行步骤1的回调run
3.PullAPIWrapper.processPullResult 取出匹配TAG的消息存入 PullResult.msgFoundList列表
4.执行步骤1后续流程，进行GroovyScript匹配
5.调用processQueue.putMessage把接收的msg存入 processQueue.msgTreeMap 队列
5.调用 ConsumeMessageConcurrentlyService.submitConsumeRequest 启动延迟消费，时间到后执行 ConsumeMessageConcurrentlyService.submitConsumeRequest
6. 在步骤5的延时时间到后，执行ConsumeMessageConcurrentlyService.submitConsumeRequest，触发线程池处理消费步骤5中的分批消费任务
7. 通过线程调度执行 ConsumeRequest.run

数据通信报文类型参考:RequestCode  通信协议中的data header部分的code: 就是用的该类中的各种协议类型

每个客户端消费者分组对每一个topic有一个线程来从broker拉取该topic下面的队列消息，在客户端每个消费者分组从对应的每个queue中最多拉取1000条消息
例如现在有一个topic，有2个queue，只有一个消费者分组consumer消费该队列信息，则客户端会有 2个 PullRequest 类来分别从broker的2个queue拉取消息，这样本低最多存2000条消息(从每个queue最多拉1000)
条，两个queue就是2000条。 如果consumer消费分组消费失败，则会把消息打回到broker中的重试队列：topic名为:%RETRY%consumer，队列数为每个broker上面一个queue 。
nameserver就会感知到这些重试队列。
下次客户端从nameserver获取queue信息的时候就会获取到有新的重试队列在该broker上,这样客户端通过向nameserver查询也就感知到了，然后新增对应PullRequest
类来拉取重试队列中的消息。 客户端消费失败后，失败的消息又会打回到broker，对broker来说是新的消息(msgid会发生变化)，实际上是之前消费失败的消息，如果反复失败，
则重试队列上面会有多条相同的msg(msg body一样，msgid是不一样的，对于broker来收每次收到一条msg都任务是一条新的msg)

通信协议:length header(4) + length(4) + body内容  见手册9.1节
length header(4) + length(4) + {"code":15,"extFields":{"topic":"%RETRY%proxy-on-trade","queueId":"0","consumerGroup":"proxy-on-trade","commitOffset":"18"},"flag":2,"language":"JAVA","opaque":124131,"serializeTypeCurrentRPC":"JSON","version":115}
    * */

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

/*
* 客户端工作原理要点:
* 1. 客户端通过PullRequest(对于一个broker上的topic queue)实时从对于的topic queue中拉取消息消费，拉取消息后先存入客户端本地，
*    如果拉取到的消息超过1000条，则过一段时间再继续拉取。
* 2. 客户端消费消息成功后会向后移动每个queue的offset，然后通过offset更新线程定时上报给broker，broker收到后就会更新位点
* 3. 如果客户端消费失败，则会发一条打回消息给broker(携带有消费失败这条消息在commitlog文件中的offset),broker收到后就可以通过
*    该commitlogoffset找到之前消费失败的msg，然后从其中取出相关信息，创建一条新的msg，然后写入重试队列(如果没有超过最大消费次数)。
*    注意消费失败的原有消息的位点offset还是会移动的，因此下次不会再拉取之前消费失败的消息，而是拉取打回消息后broker在重试队列中新建的消息。
*
*    注意:在打回消息broker收到后，broker首先判断该消息消费失败的次数，如果没有超过重试最大消费次数，则根据源msg新建的msg会写入
*    重试队列，如果超过最大重试消费次数，则创建死信队列，写入死信队列
* */
public class PushConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("yyzGroup2");
        consumer.setNamesrvAddr("10.2.223.157:9876;10.2.223.158:9876;10.2.223.159:9876");
       // consumer.setNamesrvAddr("10.2.223.228:9876");
        //consumer.subscribe("my-topic-2", "*", new GroovyScript(groovyScript));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //consumer.setMessageModel(MessageModel.CLUSTERING);
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
