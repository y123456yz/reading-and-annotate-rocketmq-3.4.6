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

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**  协议格式:<length> <header length> <header data> <bodydata>
 *所有的通信协议列表见 RequestCode，通过 createRequestCommand 来构建通信内容，然后通过 NettyEncoder.encode 进行序列化，然后发送
 *服务端收到后通过 NettyDecoder.decode反序列号，然后NettyServerHandler读取反序列号后的报文，
 * 数据收发 请求 应答对应的分支在 RemotingCommandType（NettyRemotingAbstract.processMessageReceived）
 * 接收到 RemotingCommand 在 NettyDecoder.decode 中生成
 *
 * @author shijia.wxr  RocketMq服务器与客户端通过传递RemotingCommand来交互，通过 NettyDecoder，对RemotingCommand进行协议的编码与解码
 *
 */
/**
 * @author shijia.wxr
 * RocketMq服务器与客户端通过传递 RemotingCommand 来交互，通过NettyDecoder，对RemotingCommand进行协议的编码与解码
 *
 *  数据收发 请求 应答对应的分支在 RemotingCommandType（NettyRemotingAbstract.processMessageReceived）
//NettyRemotingClient 和 NettyRemotingServer 中的initChannel执行各种命令回调

 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
    private static final int FRAME_MAX_LENGTH = //
            Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "8388608"));


    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    //最终在 RemotingCommand.decode 中进行解析通信报文， encode为组对应的通信报文，
    //这里解析出的RemotingCommand在 NettyRemotingServer.initChannel NettyRemotingClient.initChannel中的相关handler会用到
    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();

            //RemotingCommand 的encode和decode接口完成通信报文的序列化和反序列化
            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}
