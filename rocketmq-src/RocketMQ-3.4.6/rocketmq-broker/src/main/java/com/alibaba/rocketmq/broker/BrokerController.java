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

import com.alibaba.rocketmq.broker.client.*;
import com.alibaba.rocketmq.broker.client.net.Broker2Client;
import com.alibaba.rocketmq.broker.client.rebalance.RebalanceLockManager;
import com.alibaba.rocketmq.broker.filtersrv.FilterServerManager;
import com.alibaba.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import com.alibaba.rocketmq.broker.longpolling.PullRequestHoldService;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.broker.offset.ConsumerOffsetManager;
import com.alibaba.rocketmq.broker.out.BrokerOuterAPI;
import com.alibaba.rocketmq.broker.processor.*;
import com.alibaba.rocketmq.broker.slave.SlaveSynchronize;
import com.alibaba.rocketmq.broker.subscription.SubscriptionGroupManager;
import com.alibaba.rocketmq.broker.topic.TopicConfigManager;
import com.alibaba.rocketmq.common.*;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageArrivingListener;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.stats.BrokerStats;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;


/**
 * 每一个broker默认会启动三个端口。
 * 10909 broker的所谓netty fast server的地址， 和10911端口一样，用于处理跟nameserver和client的通信。
 * 10911 broker的netty server的地址， 用于和namserver 以及client的通信。
 * 10912   master broker 用于主备数据同步的端口。
 *
 *BrokerController的556行会启动一个定时服务，每间隔30s由Broker向NameServer发送注册请求,
 * NameServer会在RouteInfoManager的199行向 brokerLiveTable 写入broker的live 信息.
 * NameServer会在RouteInfoManager的523行，每10s做一次过期扫描， 并以2分钟作为broker过期时间, 一旦过期，则把
 *
 *
 * @author shijia.wxr
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final DataVersion configDataVersion = new DataVersion();
    private final ConsumerOffsetManager consumerOffsetManager; //消费进度consumer offset
    private final ConsumerManager consumerManager;
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    private final MessageArrivingListener messageArrivingListener;
    private final Broker2Client broker2Client;
    private final SubscriptionGroupManager subscriptionGroupManager; //消费者订阅关系consumer subscription
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "BrokerControllerScheduledThread"));
    private final SlaveSynchronize slaveSynchronize;
    //this.messageStore = new DefaultMessageStore() ，见 initialize 接口
    private MessageStore messageStore;
    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;

    private TopicConfigManager topicConfigManager; //topic配置
    ////消息发送线程池 。 赋值见 BrokerController.initialize
    private ExecutorService sendMessageExecutor;
    ////消息拉取线程池。 赋值见 BrokerController.initialize
    private ExecutorService pullMessageExecutor;
    //admin管理 线程池。赋值见 BrokerController.initialize
    private ExecutorService adminBrokerExecutor;
    ////client管理线程池。赋值见 BrokerController.initialize
    private ExecutorService clientManageExecutor;
    private boolean updateMasterHAServerAddrPeriodically = false;

    private BrokerStats brokerStats;
    private final BlockingQueue<Runnable> sendThreadPoolQueue;

    private final BlockingQueue<Runnable> pullThreadPoolQueue;

    private final FilterServerManager filterServerManager;

    private final BrokerStatsManager brokerStatsManager;
    private InetSocketAddress storeHost;


    public BrokerController(//
            final BrokerConfig brokerConfig, //
            final NettyServerConfig nettyServerConfig, //
            final NettyClientConfig nettyClientConfig, //
            final MessageStoreConfig messageStoreConfig //
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        this.slaveSynchronize = new SlaveSynchronize(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));
    }


    public boolean initialize() throws CloneNotSupportedException {
        boolean result = true;

        /* 加载/root/store/config中的各个配置文件到响应的类中 */
        result = result && this.topicConfigManager.load(); //加载topic配置
        result = result && this.consumerOffsetManager.load(); // 加载消费进度consumer offset
        result = result && this.subscriptionGroupManager.load(); //加载消费者订阅关系consumer subscription

        if (result) {
            try {
                this.messageStore =
                        new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                            this.brokerConfig);
            }
            catch (IOException e) {
                result = false;
                e.printStackTrace();
            }
        }

        //加载本地消息 DefaultMessageStore.load()  delayOffset.json  consumerOffset.json  commitlog  consumequeue加载
        result = result && this.messageStore.load(); //DefaultMessageStore.load

        if (result) {
            //对生产者和消费者进行网络数据收发的 10911端口 的网络事件处理
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            NettyServerConfig fastConfig=(NettyServerConfig) this.nettyServerConfig.clone();
            fastConfig.setListenPort(nettyServerConfig.getListenPort()-2);
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
            //消息发送线程池 。
            this.sendMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getSendMessageThreadPoolNums(),//
                this.brokerConfig.getSendMessageThreadPoolNums(),//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                this.sendThreadPoolQueue,//
                new ThreadFactoryImpl("SendMessageThread_"));

            //消息拉取线程池。
            this.pullMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getPullMessageThreadPoolNums(),//
                this.brokerConfig.getPullMessageThreadPoolNums(),//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                this.pullThreadPoolQueue,//
                new ThreadFactoryImpl("PullMessageThread_"));

            //admin管理 线程池。
            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                        "AdminBrokerThread_"));

            //client管理线程池。
            this.clientManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getClientManageThreadPoolNums(), new ThreadFactoryImpl(
                        "ClientManageThread_"));

            this.registerProcessor();

            this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);

            // TODO remove in future
            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            //scheduleAtFixedRate方法：“fixed-rate”；如果第一次执行时间被delay了，随后的执行时间按照 上一次开始的 时间点 进行计算
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    }
                    catch (Exception e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS); //每24小时记录一次broker stats

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    }
                    catch (Exception e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS); //每5秒flush一次消费位点。

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        log.info("dispatch behind commit log {} bytes", BrokerController.this.getMessageStore().dispatchBehindBytes());
                    }
                    catch (Exception e) {
                        log.error("schedule dispatchBehindBytes error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS); //  每1分钟记录一次分发到cq和索引文件中的commitlog的位点和已经commit的位点差异。。

            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            }
            else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr(); //定时从http rest api 获取NS地址
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) { //本broker是从broker
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                }
                else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.slaveSynchronize.syncAll();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask syncAll slave exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
            else { //本broker是主broker
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            //打印主备之间commitlog同步位点差异。
                            BrokerController.this.printMasterAndSlaveDiff();
                        }
                        catch (Exception e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        return result;
    }


    /**
     * 每一个请求码都在remoting 和 fast remoting两个server上同时有处理。
     * 通信协议注册  赋值见 BrokerController.initialize
     */
    public void registerProcessor() { ////NettyRemotingClient 和 NettyRemotingServer 中的initChannel执行各种命令回调
        //processor 可能是SendMessageProcessor ClientManageProcessor SendMessageProcessor QueryMessageProcessor ClientManageProcessor
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor,this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor,this.sendMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor,this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor,this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor,this.sendMessageExecutor);

        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        clientProcessor.registerConsumeMessageHook(this.consumeMessageHookList);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);


        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);

        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);

        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);

        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }


    public Broker2Client getBroker2Client() {
        return broker2Client;
    }


    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }


    public String getConfigDataVersion() {
        return this.configDataVersion.toJson();
    }


    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }


    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }


    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public ProducerManager getProducerManager() {
        return producerManager;
    }

	public void setFastRemotingServer(RemotingServer fastRemotingServer) {
		this.fastRemotingServer = fastRemotingServer;
	}
    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }


    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }


    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer!=null) {
			this.fastRemotingServer.shutdown();
		}
        
        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
        }

        //把broker从NS注销掉。
        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }
    }


    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId());
    }


    public String getBrokerAddr() {
        String addr = this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
        return addr;
    }


    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

		if (this.fastRemotingServer != null) {
			this.fastRemotingServer.start();
		}
		
        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        this.registerBrokerAll(true, false);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false);
                }
                catch (Exception e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }
    }


    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        /**
         * broker本身不可读或者不可写，则用 broker 本身的权限配置覆盖topic的读写权限。
         * 或者简单说 ，broker的读写权限比topic的读写权限具有更高的优先级。
         *
         */
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                            this.brokerConfig.getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

//        把broker维护的topic配置推送给namserver, 同时把broker注册到Nameserver
        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId(), //
            this.getHAServerAddr(), //
            topicConfigWrapper,//
            this.filterServerManager.buildNewFilterServerList(),//
            oneway);

        if (registerBrokerResult != null) {
            //registerBrokerResult.getHaServerAddr() != null 标识当前broker是slave，则需要把ha server的地址更新掉。
            //所谓ha是 master broker用于主从数据同步的IP + 端口。
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            if (checkOrderConfig) {
                this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
        }
    }


    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }


    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }


    public String getHAServerAddr() {
        String addr = this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
        return addr;
    }


    public void updateAllConfig(Properties properties) {
        MixAll.properties2Object(properties, brokerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, nettyClientConfig);
        MixAll.properties2Object(properties, messageStoreConfig);
        this.configDataVersion.nextVersion();
        this.flushAllConfig();
    }


    private void flushAllConfig() {
        String allConfig = this.encodeAllConfig();
        try {
            MixAll.string2File(allConfig, BrokerPathConfigHelper.getBrokerConfigPath());
            log.info("flush broker config, {} OK", BrokerPathConfigHelper.getBrokerConfigPath());
        }
        catch (IOException e) {
            log.info("flush broker config Exception, " + BrokerPathConfigHelper.getBrokerConfigPath(), e);
        }
    }


    public String encodeAllConfig() {
        StringBuilder sb = new StringBuilder();
        {
            Properties properties = MixAll.object2Properties(this.brokerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.messageStoreConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyServerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyClientConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }
        return sb.toString();
    }


    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }


    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }


    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }


    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }


    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }


    public BrokerStats getBrokerStats() {
        return brokerStats;
    }


    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }


    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }


    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }


    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }


    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("slave fall behind master, how much, {} bytes", diff);
    }

    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();


    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();


    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }


    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }


    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }


    public InetSocketAddress getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }
}
