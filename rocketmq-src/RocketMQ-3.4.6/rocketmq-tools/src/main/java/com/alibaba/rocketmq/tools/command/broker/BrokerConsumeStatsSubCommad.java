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
package com.alibaba.rocketmq.tools.command.broker;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.ConsumeStatsList;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.*;


/**
 *  注意sh mqadmin brokerConsumeStats(类BrokerConsumeStatsSubCommad) 和sh mqadmin ConsumeStats(ConsumerStatusSubCommand)的区别
 * [root@x-x-5 bin]# sh mqadmin brokerConsumeStats -b  10.2.223.165:10911 -n 10.2.223.158:9876 -t 1000
 #Topic                            #Group                            #Broker Name                      #QID  #Broker Offset        #Consumer Offset      #Diff                 #LastTime
 %RETRY%umbrella-binlog-consumer-  umbrella-binlog-consumer-group    broker-a                          0     19                    19                    0                     2017-03-29 10:18:49
 %RETRY%tradeWaitShippingGroupNam  tradeWaitShippingGroupName        broker-a                          0     68                    68                    0                     2017-03-27 20:21:54
 topic-prod-tradeservice-do        tradeWaitShippingGroupName        broker-a                          0     141                   141                   0                     2017-04-01 10:52:40
 topic-prod-tradeservice-do        tradeWaitShippingGroupName        broker-a                          1     126                   126                   0                     2017-03-28 17:42:58
 topic-prod-tradeservice-do        tradeWaitShippingGroupName        broker-a                          2     124                   124                   0                     2017-03-28 17:42:58
 topic-prod-tradeservice-do        tradeWaitShippingGroupName        broker-a                          3     122                   122                   0                     2017-03-28 17:43:03
 topic_checkin_remind              CheckinRemind_16_10               broker-a                          0     32                    28                    4                     2017-03-16 21:00:47
 topic_checkin_remind              CheckinRemind_16_10               broker-a                          1     31                    27                    4                     2017-03-16 21:00:47
 topic_checkin_remind              CheckinRemind_16_10               broker-a                          2     28                    24                    4                     2017-03-16 21:00:28
 topic_checkin_remind              CheckinRemind_16_10               broker-a                          3     26                    22                    4                     2017-03-16 21:00:28
 %RETRY%CheckinRemind_16_10        CheckinRemind_16_10               broker-a                          0     9                     9                     0                     2017-03-16 16:34:22
 topic-prod-member-checkin         CheckinRemind_16_30               broker-a                          0     20                    20                    0                     2017-03-24 15:55:57
 topic-prod-member-checkin         CheckinRemind_16_30               broker-a                          1     20                    20                    0                     2017-03-24 15:55:57
 topic-prod-member-checkin         CheckinRemind_16_30               broker-a                          2     12                    12                    0                     2017-03-23 16:58:29
 topic-prod-member-checkin         CheckinRemind_16_30               broker-a                          3     12                    12                    0                     2017-03-23 16:58:36
 topic-prod-member-checkin         CheckinRemind_16_30               broker-a                          4     8                     8                     0                     2017-03-23 17:00:49
 topic-prod-member-checkin         CheckinRemind_16_30               broker-a                          5     8                     8                     0                     2017-03-23 17:00:49
 topic-prod-promotion-coupon       risk_event_2                      broker-a                          0     245                   239                   6                     2017-04-17 13:23:12
 topic-prod-promotion-coupon       risk_event_2                      broker-a                          1     297                   294                   3                     2017-04-17 13:57:31
 topic-prod-promotion-coupon       risk_event_2                      broker-a                          2     255                   252                   3                     2017-04-17 13:33:29
 topic-prod-promotion-coupon       risk_event_2                      broker-a                          3     286                   282                   4                     2017-04-17 13:55:54
 topic-prod-promotion-coupon       risk_event                        broker-a                          0     245                   239                   6                     2017-04-17 13:23:12
 topic-prod-promotion-coupon       risk_event                        broker-a                          1     297                   293                   4                     2017-04-17 13:05:16
 topic-prod-promotion-coupon       risk_event                        broker-a                          2     255                   252                   3                     2017-04-17 13:33:29
 topic-prod-promotion-coupon       risk_event                        broker-a                          3     286                   280                   6                     2017-04-17 13:20:33
 topic-prod-member-checkin         CheckinRemind_15_30               broker-a                          0     20                    20                    0                     2017-03-24 15:55:57
 topic-prod-member-checkin         CheckinRemind_15_30               broker-a                          1     20                    20                    0                     2017-03-24 15:55:57
 *
 * @author shijia.wxr
 */
public class BrokerConsumeStatsSubCommad implements SubCommand {
    /* brokerStatus为整个broker的一些全局统计信息， brokerConsumeStats为broker下面所有消费队列的一些消费统计等信息 */
    @Override
    public String commandName() {
        return "brokerConsumeStats";
    }


    @Override
    public String commandDesc() {
        return "Fetch broker consume stats data";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "Broker address");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "timeoutMillis", true, "request timeout Millis");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("l", "level", true, "threshold of print diff");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "order", true, "order topic");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String brokerAddr = commandLine.getOptionValue('b').trim();
            boolean isOrder = false;
            long timeoutMillis = 50000;
            long diffLevel = 0;
            if (commandLine.hasOption('o')) { //-o 参数指定ture or false,例如sh mqadmin brokerConsumeStats -b  10.2.x.x:10911 -n 10.2.x.x:9876 -t 1000  -o ture
                isOrder = Boolean.parseBoolean(commandLine.getOptionValue('o').trim());
            }
            if (commandLine.hasOption('t')) {
                timeoutMillis = Long.parseLong(commandLine.getOptionValue('t').trim());
            }
            if (commandLine.hasOption('l')) {
                //diffLevel的作用是，只把某个消费者分组消息积压数量小于diffLevel的相关队列信息打印出来
                diffLevel = Long.parseLong(commandLine.getOptionValue('l').trim());
            }

            //按照消费者分组获取各个消费者分组消费topic中对应queue的消费位点信息
            //sh mqadmin brokerConsumeStats xx 命令的执行流程在这里面，获取topic下所有消费分组消费的队列的位点消费详情信息
            ConsumeStatsList consumeStatsList = defaultMQAdminExt.fetchConsumeStatsInBroker(brokerAddr, isOrder, timeoutMillis);
            System.out.printf("%-32s  %-32s  %-32s  %-4s  %-20s  %-20s  %-20s  %s\n",//
                    "#Topic",//
                    "#Group",//
                    "#Broker Name",//
                    "#QID",//
                    "#Broker Offset",//
                    "#Consumer Offset",//
                    "#Diff", //
                    "#LastTime");
            for (Map<String, List<ConsumeStats>> map : consumeStatsList.getConsumeStatsList()){
                for (String group : map.keySet()){
                    List<ConsumeStats> consumeStatsArray = map.get(group); //消费者分组
                    for (ConsumeStats consumeStats : consumeStatsArray){ //该消费者分组对应消费的各个topic下的queue的消费位点等信息
                        List<MessageQueue> mqList = new LinkedList<MessageQueue>();
                        mqList.addAll(consumeStats.getOffsetTable().keySet()); //取出所有的MessageQueue ，存入mqList
                        Collections.sort(mqList);
                        for (MessageQueue mq : mqList) {
                            OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);
                            long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
                            if (diff < diffLevel){ //diffLevel的作用是，只把某个消费者分组消息积压数量小于diffLevel的相关队列信息打印出来
                                continue;
                            }
                            String lastTime = "-";
                            try {
                                lastTime = UtilAll.formatDate(new Date(offsetWrapper.getLastTimestamp()), UtilAll.yyyy_MM_dd_HH_mm_ss);
                            }
                            catch (Exception e) {
                                //
                            }
                            if (offsetWrapper.getLastTimestamp() > 0)
                                System.out.printf("%-32s  %-32s  %-32s  %-4d  %-20d  %-20d  %-20d  %s\n",//
                                        UtilAll.frontStringAtLeast(mq.getTopic(), 32),//
                                        group,
                                        UtilAll.frontStringAtLeast(mq.getBrokerName(), 32),//
                                        mq.getQueueId(),//
                                        offsetWrapper.getBrokerOffset(),//
                                        offsetWrapper.getConsumerOffset(),//
                                        diff, //
                                        lastTime//
                                );
                        }
                    }
                }
            }
            System.out.println();
            System.out.printf("Diff Total: %d\n", consumeStatsList.getTotalDiff());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
