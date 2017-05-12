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
package com.alibaba.rocketmq.filtersrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.conflict.PackageConflictDetect;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.netty.NettySystemConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


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
 *
 * @author shijia.wxr
 */
public class FiltersrvStartup {
    public static Logger log;


    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Filter server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static void main(String[] args) {
        start(createController(args));
    }


    public static FiltersrvController start(FiltersrvController controller) {
        try {
            controller.start();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        String tip = "The Filter Server boot success, " + controller.localAddr();
        log.info(tip);
        System.out.println(tip);

        return controller;
    }


    public static FiltersrvController createController(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.SocketSndbufSize = 65535;
        }

        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.SocketRcvbufSize = 1024;
        }

        try {
            PackageConflictDetect.detectFastjson();

            Options options = ServerUtil.buildCommandlineOptions(new Options());
            final CommandLine commandLine =
                    ServerUtil.parseCmdLine("mqfiltersrv", args, buildCommandlineOptions(options),
                        new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            final FiltersrvConfig filtersrvConfig = new FiltersrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();

            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    Properties properties = new Properties();
                    properties.load(in);
                    MixAll.properties2Object(properties, filtersrvConfig);
                    System.out.println("load config properties file OK, " + file);
                    in.close();

                    String port = properties.getProperty("listenPort");
                    if (port != null) {
                        filtersrvConfig.setConnectWhichBroker(String.format("127.0.0.1:%s", port));
                    }
                }
            }

            nettyServerConfig.setListenPort(0);

            nettyServerConfig.setServerAsyncSemaphoreValue(filtersrvConfig.getFsServerAsyncSemaphoreValue());
            nettyServerConfig.setServerCallbackExecutorThreads(filtersrvConfig
                .getFsServerCallbackExecutorThreads());
            nettyServerConfig.setServerWorkerThreads(filtersrvConfig.getFsServerWorkerThreads());

            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, filtersrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), filtersrvConfig);

            if (null == filtersrvConfig.getRocketmqHome()) {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(filtersrvConfig.getRocketmqHome() + "/conf/logback_filtersrv.xml");
            log = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);

            final FiltersrvController controller =
                    new FiltersrvController(filtersrvConfig, nettyServerConfig);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);


                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        }
        catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }
}
