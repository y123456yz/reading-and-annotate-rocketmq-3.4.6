#!/bin/sh

sh ./runclass.sh -Dcom.alibaba.rocketmq.client.sendSmartMsg=true com.alibaba.rocketmq.example.benchmark.Producer $@ &
