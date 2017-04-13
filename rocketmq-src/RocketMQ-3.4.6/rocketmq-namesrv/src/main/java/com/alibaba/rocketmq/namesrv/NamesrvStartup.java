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
package com.alibaba.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.conflict.PackageConflictDetect;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
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
 * @author shijia.wxr
 */
public class NamesrvStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;


    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static void main(String[] args) {
        main0(args);
    }


    public static NamesrvController main0(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.SocketSndbufSize = 2048;
        }

        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.SocketRcvbufSize = 1024;
        }

        try {
            PackageConflictDetect.detectFastjson();

            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine =
                    ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options),
                        new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            final NamesrvConfig namesrvConfig = new NamesrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            /*
            *
            [xxx.yang@s10-2-x-x logs]$ ps -ef | grep names
root     122828      1  0  2016 ?        00:00:00 sh /opt/rocketmq/alibaba-rocketmq/bin/mqnamesrv -c /opt/rocketmq/alibaba-rocketmq/conf/nameserver-config.properties
root     122839 122828  0  2016 ?        00:00:00 sh /opt/rocketmq/alibaba-rocketmq/bin/runserver.sh com.alibaba.rocketmq.namesrv.NamesrvStartup -c /opt/rocketmq/alibaba-rocketmq/conf/nameserver-config.properties
root     122841 122839  0  2016 ?        02:27:23 /opt/jdk/jdk1.7.0_71/bin/java -server -Xms8g -Xmx8g -Xmn4g -XX:PermSize=1g -XX:MaxPermSize=1g -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC -verbose:gc -Xloggc:/rmq_srv_gc.log -XX:+PrintGCDetails -XX:-OmitStackTraceInFastThrow -Djava.ext.dirs=/opt/rocketmq/alibaba-rocketmq/bin/../lib -Duser.home=/data -cp .:/opt/rocketmq/alibaba-rocketmq/bin/../conf: com.alibaba.rocketmq.namesrv.NamesrvStartup -c /opt/rocketmq/alibaba-rocketmq/conf/nameserver-config.properties
            [yazhou.yang@s10-2-x-x logs]$
            [yazhou.yang@s10-2-x-x logs]$
            [yazhou.yang@s10-2-x-x logs]$ cat /opt/rocketmq/alibaba-rocketmq/conf/nameserver-config.properties
            listenPort=9998
            * */ /* 默认是9876端口，但是可以通过启动 mqnamesrv 的时候加上-c参数指定配置文件，指定端口 */
            nettyServerConfig.setListenPort(9876);
            if (commandLine.hasOption('c')) { //启动的时候带有-c参数
                String file = commandLine.getOptionValue('c');
                if (file != null) { //获取对应的配置文件信息
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    MixAll.properties2Object(properties, namesrvConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }

            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, namesrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

            if (null == namesrvConfig.getRocketmqHome()) {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
            final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

            MixAll.printObjectProperties(log, namesrvConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);

            final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);
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

            controller.start(); //NamesrvController.start

            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.println(tip);

            return controller;
        }
        catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }
}
