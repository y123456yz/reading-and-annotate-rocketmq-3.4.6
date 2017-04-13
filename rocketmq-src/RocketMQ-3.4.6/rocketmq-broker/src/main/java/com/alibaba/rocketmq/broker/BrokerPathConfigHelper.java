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

package com.alibaba.rocketmq.broker;

import java.io.File;


public class BrokerPathConfigHelper {
    private static String brokerConfigPath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "config" + File.separator + "broker.properties";


    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }


    public static void setBrokerConfigPath(String path) {
        brokerConfigPath = path;
    }

    /*
    * "topicYYZ10":{
                "order":false,
                "perm":6,
                "readQueueNums":10,
                "readable":true,
                "topicFilterType":"SINGLE_TAG",
                "topicName":"topicYYZ10",
                "topicSysFlag":0,
                "writable":true,
                "writeQueueNums":10
        },
    * */
    //root/store/config/topics.json  这里面存储的是各种topic信息，如上
    public static String getTopicConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "topics.json";
    }

    /*
    *  "topicyyz10@topicyyz10ConsumerGroup":{0:89897052,2:89897210,1:69860336,3:69857016
        },

        /root/store/config/consumerOffset.json   每个消费分组在topic上的消费位点信息
    * */
    public static String getConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerOffset.json";
    }

    /*
    "topic-test-2ConsumerGroup":{
            "brokerId":0,
            "consumeBroadcastEnable":true,
            "consumeEnable":true,
            "consumeFromMinEnable":true,
            "groupName":"topic-test-2ConsumerGroup",
            "retryMaxTimes":16,
            "retryQueueNums":1,
            "whichBrokerWhenConsumeSlowly":1
    },
    * 订阅消费分组信息
    * /root/store/config/subscriptionGroup.json
    * */
    public static String getSubscriptionGroupPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "subscriptionGroup.json";
    }

}
