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
package com.alibaba.rocketmq.broker.subscription;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.BrokerPathConfigHelper;
import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author shijia.wxr  消费者订阅关系consumer subscription
 */
public class SubscriptionGroupManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private transient BrokerController brokerController;

    //把/root/store/config/subscriptionGroup.json文件中的信息序列化到这里
    private final ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
            new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);
    //标识subscriptionGroupTable 的数据版本。
    private final DataVersion dataVersion = new DataVersion();


    private void init() {
        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.ONS_HTTP_PROXY_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PULL_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PERMISSION_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_OWNER_GROUP, subscriptionGroupConfig);
        }
    }


    public SubscriptionGroupManager() {
        this.init();
    }


    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }


    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: " + old + " new: " + config);
        }
        else {
            log.info("create new subscription group, " + config);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }


    /**
     * 查找某一个消费者分组的订阅配置。
     * @param group
     * @return
     */
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupConfig) {
            //如果broker配置可以自动创建订阅者分组或者消费者分组名以 CID_RMQ_SYS_ 开头。
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                this.subscriptionGroupTable.putIfAbsent(group, subscriptionGroupConfig);
                log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                this.dataVersion.nextVersion();
                //把订阅配置信息序列化成json写入配置文件目录。
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }


    @Override
    public String encode() {
        return this.encode(false);
    }


    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }


    @Override //把/root/store/config/subscriptionGroup.json文件中的信息序列化到 SubscriptionGroupManager.subscriptionGroupTable
    public void decode(String jsonString) { //ConfigManager.configFilePath中执行
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }


    @Override
    public String configFilePath() { //ConfigManager.configFilePath中执行
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }


    public ConcurrentHashMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }


    public DataVersion getDataVersion() {
        return dataVersion;
    }


    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group: " + old);
            this.dataVersion.nextVersion();
            this.persist();
        }
        else {
            log.warn("delete subscription group failed, subscription group: " + old + " not exist");
        }
    }
}
