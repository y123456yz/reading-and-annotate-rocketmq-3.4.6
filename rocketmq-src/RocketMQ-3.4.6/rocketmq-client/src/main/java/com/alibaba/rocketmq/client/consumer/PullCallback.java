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
package com.alibaba.rocketmq.client.consumer;

/**
 * Async message pulling interface
 *
 * @author shijia.wxr   在pullMessage中会new一个类，并实现该接口
 */
public interface PullCallback {
    //pullMessageAsync中拉取消息成功的时候执行
    void onSuccess(final PullResult pullResult);

    //pullMessageAsync中拉取消息异常的时候执行
    void onException(final Throwable e);
}
