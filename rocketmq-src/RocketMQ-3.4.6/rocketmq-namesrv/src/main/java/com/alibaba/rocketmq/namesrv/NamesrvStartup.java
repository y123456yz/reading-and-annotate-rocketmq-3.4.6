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
 * 4，Broker与Namesrv的心跳机制
 单个Broker跟所有Namesrv保持心跳请求，心跳间隔为30秒，心跳请求中包括当前Broker所有的Topic信息。
 Namesrv会反查Broer的心跳信息，如果某个Broker在2分钟之内都没有心跳，则认为该Broker下线，调整Topic
 跟Broker的对应关系。但此时Namesrv不会主动通知Producer、Consumer有Broker宕机。
 *
 *
 * 消费者启动时需要指定Namesrv地址，与其中一个Namesrv建立长连接。消费者每隔30秒从nameserver获取所有topic
 * 的最新队列情况，这意味着某个broker如果宕机，客户端最多要30秒才能感知。连接建立后，从namesrv中获取当前
 * 消费Topic所涉及的Broker，直连Broker。
 * Consumer跟Broker是长连接，会每隔30秒发心跳信息到Broker。Broker端每10秒检查一次当前存活的Consumer，若发
 * 现某个Consumer 2分钟内没有心跳，就断开与该Consumer的连接，并且向该消费组的其他实例发送通知，触发该消费者集群的负载均衡。
 *
 * Producer启动时，也需要指定Namesrv的地址，从Namesrv集群中选一台建立长连接。如果该Namesrv宕机，会自动连其他Namesrv。直到有可用的Namesrv为止。
 * 生产者每30秒从Namesrv获取Topic跟Broker的映射关系，更新到本地内存中。再跟Topic涉及的所有Broker建立长连接，每隔30秒发一次心跳。
 * 在Broker端也会每10秒扫描一次当前注册的Producer，如果发现某个Producer超过2分钟都没有发心跳，则断开连接。
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

            /*
            * <?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

<configuration>
        <appender name="DefaultAppender"
                class="ch.qos.logback.core.rolling.RollingFileAppender">
                <file>${user.home}/logs/rocketmqlogs/namesrv_default.log</file>
                <append>true</append>
                <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
                        <fileNamePattern>${user.home}/logs/rocketmqlogs/otherdays/namesrv_default.%i.log
                        </fileNamePattern>
                        <minIndex>1</minIndex>
                        <maxIndex>5</maxIndex>
                </rollingPolicy>
                <triggeringPolicy
                        class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
                        <maxFileSize>100MB</maxFileSize>
                </triggeringPolicy>
                <encoder>
                        <pattern>%d{yyy-MM-dd HH:mm:ss,GMT+8} %p %t - %m%n</pattern>
                        <charset class="java.nio.charset.Charset">UTF-8</charset>
                </encoder>
        </appender>

        <appender name="RocketmqNamesrvAppender"
                class="ch.qos.logback.core.rolling.RollingFileAppender">
                <file>${user.home}/logs/rocketmqlogs/namesrv.log</file>
                <append>true</append>
                <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
                        <fileNamePattern>${user.home}/logs/rocketmqlogs/otherdays/namesrv.%i.log
                        </fileNamePattern>
                        <minIndex>1</minIndex>
                        <maxIndex>5</maxIndex>
                </rollingPolicy>
                <triggeringPolicy
                        class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
                        <maxFileSize>100MB</maxFileSize>
                </triggeringPolicy>
                <encoder>
                        <pattern>%d{yyy-MM-dd HH:mm:ss,GMT+8} %p %t - %m%n</pattern>
                        <charset class="java.nio.charset.Charset">UTF-8</charset>
                </encoder>
        </appender>

        <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
                <append>true</append>
                <encoder>
                        <pattern>%d{yyy-MM-dd HH\:mm\:ss,SSS} %p %t - %m%n</pattern>
                        <charset class="java.nio.charset.Charset">UTF-8</charset>
                </encoder>
        </appender>

        <logger name="RocketmqNamesrv" additivity="false">
                <level value="INFO" />
                <appender-ref ref="RocketmqNamesrvAppender" />
        </logger>

        <logger name="RocketmqCommon" additivity="false">
                <level value="INFO" />
                <appender-ref ref="RocketmqNamesrvAppender" />
        </logger>

        <logger name="RocketmqRemoting" additivity="false">
                <level value="INFO" />
                <appender-ref ref="RocketmqNamesrvAppender" />
        </logger>

        <root>
                <level value="INFO" />
                <appender-ref ref="DefaultAppender" />
        </root>
</configuration>
            * */  //nameserver依赖 RocketmqCommon 和 RocketmqRemoting，各个模块的日志级别如上配置
            //例如listenPort=9998是通过命令行携带的，则以命令行为准，即使加到配置文件中了，因为这里后执行，所以还是以命令行为准
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
            //日志文件配置信息加载
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
