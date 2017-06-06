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

package com.alibaba.rocketmq.common.stats;

import com.alibaba.rocketmq.common.UtilAll;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

////StatsItemSet.statsItemTable 中为statsName对应的统计信息，采样统计见StatsItemSet.init
public class StatsItem {
    //incBrokerPutNums  incGroupGetSize 等中赋值
    private final AtomicLong value = new AtomicLong(0); //总值
    private final AtomicLong times = new AtomicLong(0); //总次数

    //每隔1s获取最近7s钟内，每秒钟的times和value，见 samplingInMinutes，1分钟内的平均值，实际上算的是最近7s钟内的平均值，见samplingInSeconds
    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();

    //每隔1分钟记录一次这1分钟内times和value到csListHour，最多记录最近7分钟内的统计信息，1小时内的平均值，实际上算的是最近7分钟的平均值，见 samplingInMinutes
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();

    //每隔1小时记录一次这1小时内times和value到 csListDay，最多记录最近24小时内的统计信息，1天内的平均值，实际上算的是最近24小时的平均值，见samplingInHour
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    private final String statsName; //TOPIC_PUT_NUMS 等
    private final String statsKey;  //例如TOPIC_PUT_NUMS对应的是topic  如果是GROUP_GET_NUMS，对应的是statsKey = String.format("%s@%s", topic, group);
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;

    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            double avgpt = 0;
            long sum = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                sum = last.getValue() - first.getValue();
                //这里sum乘以1000的原因是，getTimestamp的单位是ms
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                long timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) { //times增加访问内，value平均增加了多少
                    avgpt = (sum * 1.0d) / (timesDiff);
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
        }

        return statsSnapshot;
    }

    //每分钟的采样结果
    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService,
            Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    public void init() {
        //秒级采样统计
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                }
                catch (Throwable e) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        //分钟级采样统计
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                }
                catch (Throwable e) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        //小时级采样统计
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                }
                catch (Throwable e) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        //每分钟打印统计信息  //见stats.log
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), //
            1000 * 60, TimeUnit.MILLISECONDS);

        //每小时打印统计信息  //见stats.log
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextHourTimeMillis() - System.currentTimeMillis()), //
            1000 * 60 * 60, TimeUnit.MILLISECONDS);

        //每天一次统计打印  //见stats.log
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                }
                catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, //
            1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }
    //见stats.log
    //每分钟打印统计信息，例如2017-06-06 00:01:00 INFO - [SNDBCK_PUT_NUMS] [topic-prod-logistics-monitor@consumer-group-refund] Stats In One Minute, SUM: 0 TPS: 0.00 AVGPT: 0.00
    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String.format("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            ss.getSum(),//
            ss.getTps(),//
            ss.getAvgpt()));
    }

    //每小时打印统计信息
    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String.format("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            ss.getSum(),//
            ss.getTps(),//
            ss.getAvgpt()));
    }

    //每隔一天打印统计信息
    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f", //
            this.statsName,//
            this.statsKey,//
            ss.getSum(),//
            ss.getTps(),//
            ss.getAvgpt()));
    }

    //每隔1s获取最近7s钟内，每秒钟的times和value，见 samplingInMinutes，1分钟内的平均值，实际上算的是最近7s钟内的平均值
    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }

    //每隔1分钟记录一次这1分钟内times和value到csListHour，最多记录最近7分钟内的统计信息，1小时内的平均值，实际上算的是最近7分钟的平均值
    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }

    //每隔1小时记录一次这1小时内times和value到 csListDay，最多记录最近24小时内的统计信息，1天内的平均值，实际上算的是最近24小时的平均值
    public void samplingInHour() {
        synchronized (this.csListDay) {
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }


    public AtomicLong getValue() {
        return value;
    }


    public String getStatsKey() {
        return statsKey;
    }


    public String getStatsName() {
        return statsName;
    }


    public AtomicLong getTimes() {
        return times;
    }
}


class CallSnapshot {
    private final long timestamp; //单位ms，见调用CallSnapshot的地方
    private final long times;

    private final long value;


    public CallSnapshot(long timestamp, long times, long value) {
        super();
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }


    public long getTimestamp() {
        return timestamp;
    }


    public long getTimes() {
        return times;
    }


    public long getValue() {
        return value;
    }
}
