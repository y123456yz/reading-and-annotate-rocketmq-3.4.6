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
package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.common.ServiceThread;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.*;


/**
 * @author shijia.wxr     NettyRemotingServer或者NettyRemotingClient实现该类中的接口
 */
public abstract class NettyRemotingAbstract {
    private static final Logger plog = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    protected final Semaphore semaphoreOneway;

    protected final Semaphore semaphoreAsync;

    /* 通过本抽象类的invokeAsyncImpl或者invokeSyncImpl接口实现Put操作，最終是通過 NettyRemotingClient.invokeAsync或者
     * NettyRemotingServer.invokeAsync調用執行該invokeAsyncImpl接口
      * */
    protected final ConcurrentHashMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256); //scanResponseTable中遍历扫描该hash中的ResponseFuture

    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    //保存了RPC处理器  BrokerController.registerProcessor 中注册各种RPC处理器
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();


    public abstract ChannelEventListener getChannelEventListener();


    public abstract RPCHook getRPCHook();


    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }

    /**
     * 把待处理的netty事件写入LinkedBlockingQueue， 交给事件处理线程异步 处理。，
     */
    class NettyEventExecuter extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int MaxSize = 10000;


        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= MaxSize) {
                this.eventQueue.add(event);
            }
            else {
                plog.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(),
                    event.toString());
            }
        }


        @Override
        public void run() {
            plog.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                        case IDLE:
                            listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                            break;
                        case CLOSE:
                            listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                            break;
                        case CONNECT:
                            listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                            break;
                        case EXCEPTION:
                            listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                            break;
                        default:
                            break;

                        }
                    }
                }
                catch (Exception e) {
                    plog.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            plog.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return NettyEventExecuter.class.getSimpleName();
        }
    }


    /**
     * 允许单向调用和异步调用的请求信号量。
     * @param permitsOneway
     * @param permitsAsync
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }


    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        //获取指定命令对应的处理器
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair =
                null == matched ? this.defaultRequestProcessor : matched;

        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        // NettyRemotingServer 或者 NettyRemotingClient 实现该类中的接口
                        RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                        if (rpcHook != null) {
                            rpcHook
                                .doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        }

                        //各种回调见  BrokerController.registerProcessor

                        //processor可能是 SendMessageProcessor ClientManageProcessor SendMessageProcessor QueryMessageProcessor ClientManageProcessor
                        //执行这些processor对应的processRequest接口   //NettyRemotingAbstract.processRequestCommand 中执行这些接口函数
                        final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        if (rpcHook != null) {
                            rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                                cmd, response);
                        }

                        if (!cmd.isOnewayRPC()) {
                            if (response != null) {
                                response.setOpaque(cmd.getOpaque());
                                response.markResponseType();
                                try {
                                    ctx.writeAndFlush(response);
                                }
                                catch (Throwable e) {
                                    plog.error("process request over, but response failed", e);
                                    plog.error(cmd.toString());
                                    plog.error(response.toString());
                                }
                            }
                            else {
                            }
                        }
                    }
                    catch (Throwable e) {
                        plog.error("process request exception", e);
                        plog.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response =
                                    RemotingCommand.createResponseCommand(
                                        RemotingSysResponseCode.SYSTEM_ERROR,//
                                        RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(cmd.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            try {
                pair.getObject2().submit(run);
            }
            catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    plog.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) //
                            + ", too many requests and system thread pool busy, RejectedExecutionException " //
                            + pair.getObject2().toString() //
                            + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response =
                            RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                                "too many requests and system thread pool busy, please try another server");
                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        }
        else { //未找到匹配reqcode 的处理器。
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                        error);
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
            plog.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }


    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
//        if(this  instanceof  NettyRemotingClient){
//            plog.info("recv response of opaque {}" , cmd.getOpaque());
//        }
        //通过opaque字段从responseTable获取应答。
        final ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);
            //在得到异步请求响应以后，把这一次异步请求的信号量释放掉。
            responseFuture.release();

            responseTable.remove(cmd.getOpaque());

            if (responseFuture.getInvokeCallback() != null) {
                boolean runInThisThread = false;
                ExecutorService executor = this.getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    responseFuture.executeInvokeCallback();
                                }
                                catch (Throwable e) {
                                    plog.warn("excute callback in executor exception, and callback throw", e);
                                }
                            }
                        });
                    }
                    catch (Exception e) {
                        runInThisThread = true;
                        plog.warn("excute callback in executor exception, maybe executor busy", e);
                    }
                }
                else {
                    runInThisThread = true;
                }

                if (runInThisThread) {
                    try {
                        responseFuture.executeInvokeCallback();
                    }
                    catch (Throwable e) {
                        plog.warn("executeInvokeCallback Exception", e);
                    }
                }
            }
            else {
                responseFuture.putResponse(cmd);
            }
        }
        else {
            plog.warn("receive response, but not matched any request, "
                    + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            plog.warn(cmd.toString());
        }
    }

    //请求 应答 通信报文分支流程在这里
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
            case REQUEST_COMMAND: //请求
                processRequestCommand(ctx, cmd);
                break;
            case RESPONSE_COMMAND: //应答
                processResponseCommand(ctx, cmd);
                break;
            default:
                break;
            }
        }
    }


    abstract public ExecutorService getCallbackExecutor();


    public void scanResponseTable() { //NettyRemotingAbstract.scanResponseTable中
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                it.remove();
                try {
                    //执行回调处理
                    rep.executeInvokeCallback();
                }
                catch (Throwable e) {
                    plog.warn("scanResponseTable, operationComplete Exception", e);
                }
                finally {
                    rep.release();
                }

                plog.warn("remove timeout request, " + rep);
            }
        }
    }


    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException {
        try {
            final ResponseFuture responseFuture =
                    new ResponseFuture(request.getOpaque(), timeoutMillis, null, null);
            this.responseTable.put(request.getOpaque(), responseFuture);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(request.getOpaque());
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    plog.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    plog.warn(request.toString());
                }
            });
            //前面的发送和异步发送处理一样， 在这里通过reponseFuture的countdownlatch实现同步阻塞等待。
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(channel),
                        timeoutMillis, responseFuture.getCause());
                }
                else {
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel),
                        responseFuture.getCause());
                }
            }

            return responseCommand;
        }
        finally {
            this.responseTable.remove(request.getOpaque());
        }
    }


    /**
     * 异步请求的调用， 涉及到通过信号量控制并发。
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     *
     * NettyRemotingClient.invokeAsync或者NettyRemotingServer.invokeAsync中執行該接口,本客戶端只有NettyRemotingClient有用
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
            final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS); //通过信号量控制异步调用的并发量。
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseFuture responseFuture =
                    new ResponseFuture(request.getOpaque(), timeoutMillis, invokeCallback, once);
            //通过消息头中的opaque实现单个tcp连接的多线程复用。
            this.responseTable.put(request.getOpaque(), responseFuture);
            try {
                //这里的代码有点绕， writeAndFlush操作是发送remoting command给remote server (broker) .
                //直接把request类对象传输出去，对端收到后在还原出该类
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) { //发送成功以后，在responseTable 这个表里面包含了若干个key, value
                            //key是对应当前套接字通道的opaque （用于tcp 连接的多线程复用） ， value是请求的应答 。

                            //这里发送成功以后，服务端处理完毕以后 ，会回送一个response.
                            //在processResponseCommand 方法中, 会接受到一个respose command ，在那里进行回调处理 . 并释放异步请求的信号量。
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        else { //请求发送失败
                            responseFuture.setSendRequestOK(false);
                        }
                        //立即把response从table中清理掉。
                        responseFuture.putResponse(null);
                        responseTable.remove(request.getOpaque());
                        try {
                            responseFuture.executeInvokeCallback();
                        }
                        catch (Throwable e) {
                            plog.warn("excute callback in writeAndFlush addListener, and callback throw", e);
                        }
                        finally {
                            responseFuture.release();
                        }

                        plog.warn("send a request command to channel <{}> failed.",
                            RemotingHelper.parseChannelRemoteAddr(channel));
                        plog.warn(request.toString());
                    }
                });
            }
            catch (Exception e) {
                responseFuture.release();
                plog.warn(
                    "send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel)
                            + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        }
        else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            }
            else {
                String info =
                        String
                            .format(
                                "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                timeoutMillis,//
                                this.semaphoreAsync.getQueueLength(),//
                                this.semaphoreAsync.availablePermits()//
                            );
                plog.warn(info);
                plog.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }


    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            plog.warn("send a request command to channel <" + channel.remoteAddress()
                                    + "> failed.");
                            plog.warn(request.toString());
                        }
                    }
                });
            }
            catch (Exception e) {
                once.release();
                plog.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        }
        else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            }
            else {
                String info =
                        String
                            .format(
                                "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                timeoutMillis,//
                                this.semaphoreAsync.getQueueLength(),//
                                this.semaphoreAsync.availablePermits()//
                            );
                plog.warn(info);
                plog.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }
}
