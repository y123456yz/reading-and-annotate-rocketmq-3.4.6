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

package com.alibaba.rocketmq.common.conflict;

/**
 * Package conflict detector
 *
 * @author shijia.wxr
 * @author vongosling
 */
public class PackageConflictDetect {
    private static boolean detectEnable = Boolean.parseBoolean(System.getProperty(
            "com.alibaba.rocketmq.packageConflictDetect.enable", "true"));

    public static void detectFastjson() {
        if (detectEnable) {
            final String fastjsonVersion = "1.2.3";
            String version = "0.0.0";
            boolean conflict = false;
            try {
                version = com.alibaba.fastjson.JSON.VERSION;
                int code = version.compareTo(fastjsonVersion);
                if (code < 0) {
                    conflict = true;
                }
            } catch (Throwable e) {
                conflict = true;
            }

            if (conflict) {
                throw new RuntimeException(
                        String
                                .format(
                                        "Your fastjson version is %s, or no fastjson, RocketMQ minimum version required: %s",//
                                        version, fastjsonVersion));
            }
        }
    }
}
