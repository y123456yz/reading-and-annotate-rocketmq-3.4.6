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
package com.alibaba.rocketmq.common;

import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * @author shijia.wxr
 */
public abstract class ConfigManager {
    private static final Logger plog = LoggerFactory.getLogger(LoggerName.CommonLoggerName);


    public abstract String encode();


    public abstract String encode(final boolean prettyFormat);


    public abstract void decode(final String jsonString);


    public abstract String configFilePath();

    /*
    * topicConfigManager.load();
      consumerOffsetManager.load();
      subscriptionGroupManager.load();
      ScheduleMessageService.load()
    * */ //加载consumerOffset.json  delayOffset.json  subscriptionGroup.json  topics.json 到相应的地方
    public boolean load() {
        String fileName = null;
        try {
            /* topicConfigManager.configFilePath(); consumerOffsetManager.configFilePath(); subscriptionGroupManager.configFilePath();
            * ScheduleMessageService.configFilePath()
            * */
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName); //获取文件内容存入jsonString
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            }
            else {
                /* topicConfigManager.decode(); consumerOffsetManager.decode(); subscriptionGroupManager.decode();
                * ScheduleMessageService.decode()
                * */
                this.decode(jsonString); //解析上面从配置文件中读取的配置信息，然后把这些配置文件中的配置序列化到响应的地方存储
                plog.info("load {} OK", fileName);
                return true;
            }
        }
        catch (Exception e) {
            plog.error("load " + fileName + " Failed, and try to load backup file", e);
            return this.loadBak();
        }
    }


    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                plog.info("load " + fileName + " OK");
                return true;
            }
        }
        catch (Exception e) {
            plog.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }


    public synchronized void persist() {
        //例如创建topic对应的this就是TopicConfigManager, TopicConfigManager.encode就是 topicConfigTable 进行序列化后的string,见 ConfigManager.createTopicInSendMessageBackMethod
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName); //存入对应的文件
            }
            catch (IOException e) {
                plog.error("persist file Exception, " + fileName, e);
            }
        }
    }
}
