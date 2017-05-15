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
package com.alibaba.rocketmq.example.transaction;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;


/*
事务处理过程具体说明：
只有在消息发送成功，并且本地操作执行成功时，才发送提交事务消息，做事务提交。
其他的情况，例如消息发送失败，直接发送回滚消息，进行回滚，或者发送消息成功，但是执行本地操作失败，也是发送回滚消息，进行回滚。

事务消息原理实现过程：
一阶段：
Producer向Broker发送1条类型为TransactionPreparedType的Prepared消息，Broker接收消息保存在CommitLog中，然后返回消息的queueOffset和
MessageId到Producer，MessageId包含有commitLogOffset（即消息在CommitLog中的偏移量，通过该变量可以直接定位到消息本身），由
于该类型的消息在保存的时候，commitLogOffset没有被保存到consumerQueue中，此时客户端通过consumerQueue取不到commitLogOffset，
所以该类型的消息无法被取到，导致不会被消费。

一阶段的过程中，Broker保存了1条消息。


二阶段：
Producer端的TransactionExecuterImpl执行本地操作，返回本地事务的状态，然后发送一条类型为TransactionCommitType或者TransactionRollbackType的
消息到Broker确认提交或者回滚，Broker通过Request中的commitLogOffset，获取到上面状态为TransactionPreparedType的消息（简称消息A），
然后重新构造一条与消息A内容相同的消息B，设置状态为TransactionCommitType或者TransactionRollbackType，然后保存。其中
TransactionCommitType类型的，会放commitLogOffset到consumerQueue中，TransactionRollbackType类型的，消息体设置为空，不会放
commitLogOffset到consumerQueue中。

如果这一阶段发送的类型为TransactionCommitType或者TransactionRollbackType失败了？怎么办？
RocketMQ会定期扫描消息集群中的事物消息，如果发现了第一阶段的Prepared消息，它会向消息发送端(生产者)确认，客户端本地事务单元是否执行成功？
如果客户端本地执行成功是回滚还是继续发送确认消息呢？RocketMQ会根据发送端设置的策略来决定是回滚还是继续发送确认消息。这样就保证了消息
发送与本地事务同时成功或同时失败。

二阶段的过程中，Broker也保存了1条消息。

总结：事务消息过程中，broker一共保存2条消息。

rocket事务处理参考:
http://www.jianshu.com/p/453c6e7ff81c
http://lifestack.cn/archives/429.html
* */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        //事务决断处理类
        TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
        producer.setCheckThreadPoolMinSize(2);
        producer.setCheckThreadPoolMaxSize(2);
        producer.setCheckRequestHoldMax(2000);
        producer.setTransactionCheckListener(transactionCheckListener);
        producer.start();

        String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
        for (int i = 0; i < 100; i++) {
            try {
                Message msg =
                        new Message("TopicTest", tags[i % tags.length], "KEY" + i,
                            ("Hello RocketMQ " + i).getBytes());
                SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, null);
                System.out.println(sendResult);

                Thread.sleep(10);
            }
            catch (MQClientException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }

        producer.shutdown();

    }
}
