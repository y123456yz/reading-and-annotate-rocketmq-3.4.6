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

package com.alibaba.rocketmq.broker.filtersrv;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.BrokerStartup;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * rocketmq默认为客户端自己进行消息过滤，如果要使用filer server高级过滤，参考example/filter/Consumer.java
 * filer server高级过滤功能原理（见开发手册 8.2  高级消息过滤   4.4  Message Filter）:
 1.  Broker 所在的机器会启劢多个 FilterServer 过滤迕程
 2.  Consumer 启劢后，会吐 FilterServer 上传一个过滤的 Java 类
 3.  Consumer 从 FilterServer 拉消息，FilterServer 将请求转収给 Broker，FilterServer 从 Broker 收到消息后，挄照
 Consumer 上传的 Java 过滤程序做过滤，过滤完成后迒回给 Consumer。

 服务端消息过滤优缺点:
 1.  使用 CPU 资源来换叏网卡流量资源
 2.  FilterServer 不 Broker 部署在同一台机器，数据通过本地回环通信，丌走网卡
 3.  一台 Broker 部署多个 FilterServer，充分利用 CPU 资源，因为单个 Jvm 难以全面利用高配的物理机 Cpu 资源
 4.  因为过滤代码使用 Java 诧言来编写，应用几乎可以做任意形式的服务器端消息过滤，例如通过 Message Header
 迕行过滤，甚至可以挄照 Message Body 迕行过滤。
 5.  使用 Java 诧言迕行作为过滤表达式是一个双刃剑，方便了应用的过滤操作，但是带来了服务器端的安全风险。
 需要应用来保证过滤代码安全，例如在过滤程序里尽可能丌做申请大内存，创建线程等操作。避免 Broker 服
 务器収生资源泄漏。
 使用方式参见 Github 例子
 https://github.com/alibaba/RocketMQ/blob/develop/rocketmq-example/src/main/java/com/alibaba/rocketmq/example/
 filter/Consumer.java
 */
//服务端消息过滤相关
public class FilterServerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    public static final long FilterServerMaxIdleTimeMills = 30000;

    private final ConcurrentHashMap<Channel, FilterServerInfo> filterServerTable =
            new ConcurrentHashMap<Channel, FilterServerInfo>(16);

    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));

    class FilterServerInfo {
        private String filterServerAddr;
        private long lastUpdateTimestamp;


        public String getFilterServerAddr() {
            return filterServerAddr;
        }


        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }


        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }


        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }


    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    FilterServerManager.this.createFilterServer();
                }
                catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    private String buildStartCommand() {
        String config = "";
        if (BrokerStartup.configFile != null) {
            config = String.format("-c %s", BrokerStartup.configFile);
        }

        if (this.brokerController.getBrokerConfig().getNamesrvAddr() != null) {
            config += String.format(" -n %s", this.brokerController.getBrokerConfig().getNamesrvAddr());
        }

        if (RemotingUtil.isWindowsPlatform()) {
            return String.format("start /b %s\\bin\\mqfiltersrv.exe %s", //
                this.brokerController.getBrokerConfig().getRocketmqHome(),//
                config);
        }
        else {
            return String.format("sh %s/bin/startfsrv.sh %s", //
                this.brokerController.getBrokerConfig().getRocketmqHome(),//
                config);
        }
    }


    public void createFilterServer() {
        int more =
                this.brokerController.getBrokerConfig().getFilterServerNums() - this.filterServerTable.size();
        String cmd = this.buildStartCommand();
        for (int i = 0; i < more; i++) {
            FilterServerUtil.callShell(cmd, log);
        }
    }


    public void registerFilterServer(final Channel channel, final String filterServerAddr) {
        FilterServerInfo filterServerInfo = this.filterServerTable.get(channel);
        if (filterServerInfo != null) {
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        }
        else {
            filterServerInfo = new FilterServerInfo();
            filterServerInfo.setFilterServerAddr(filterServerAddr);
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            this.filterServerTable.put(channel, filterServerInfo);
            log.info("Receive a New Filter Server<{}>", filterServerAddr);
        }
    }


    /**
     * Filter Server register to broker every 10s ,if over 30s,no registration info.,remove it
     */
    public void scanNotActiveChannel() {
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            long timestamp = next.getValue().getLastUpdateTimestamp();
            Channel channel = next.getKey();
            if ((System.currentTimeMillis() - timestamp) > FilterServerMaxIdleTimeMills) {
                log.info("The Filter Server<{}> expired, remove it", next.getKey());
                it.remove();
                RemotingUtil.closeChannel(channel);
            }
        }
    }


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        FilterServerInfo old = this.filterServerTable.remove(channel);
        if (old != null) {
            log.warn("The Filter Server<{}> connection<{}> closed, remove it", old.getFilterServerAddr(),
                remoteAddr);
        }
    }


    public List<String> buildNewFilterServerList() {
        List<String> addr = new ArrayList<String>();
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            addr.add(next.getValue().getFilterServerAddr());
        }
        return addr;
    }
}
