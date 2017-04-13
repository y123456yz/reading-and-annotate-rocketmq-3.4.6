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
package com.alibaba.rocketmq.client.impl.consumer;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.ServiceThread;


/**
 * Rebalance Service
 * //selectOneMessageQueue  messageQueueList 是投递消息的时候对应的topic队列，每次投递的时候乱序选择队列投递，见ROCKET开发手册7.8节
 //rebalance相关的是针对消费，例如有多个消费者消费同一个topic，该topic有10个队列，则消费者1消费1-5队列，消费者2消费6-10对了，见ROCKETMQ开发手册7-5
 * @author shijia.wxr
 */
public class RebalanceService extends ServiceThread {
    private final Logger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;


    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    private static long WaitInterval = 1000 * 10; //多久做一次rebalance

    @Override //MQClientInstance.start(this.rebalanceService.start();)中执行
    public void run() {
        log.info(this.getServiceName() + " service started");

        //rebalance线程循环处理
        while (!this.isStoped()) { //每10秒钟做一次rebalance服务: 这个rebalance服务按照topic做rebalance ,
            //当client在指定的消费分组中需要新增对topic的队列的消费时，则提交一个 pull request给PullMessageService

            this.waitForRunning(WaitInterval); //延时
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
