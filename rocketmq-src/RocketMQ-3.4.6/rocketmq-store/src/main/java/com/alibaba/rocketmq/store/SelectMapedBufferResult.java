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
package com.alibaba.rocketmq.store;

import java.nio.ByteBuffer;


/**
 * @author shijia.wxr  SelectMapedBufferResult类获取到的是mapedFile中从startOffset开始的size字节数据，这size字节数据存入byteBuffer，见MapedFile.selectMapedBuffer
 */
public class SelectMapedBufferResult {
    //mapfile的物理起始位点
    private final long startOffset;
    //读取到的bytebuffer.
    private final ByteBuffer byteBuffer;
    //字节数组的长度。
    private int size;
    //从哪个mapfile文件
    private MapedFile mapedFile;

    //返回mapedFile中从startOffset开始的size字节数据，并存入byteBuffer
    public SelectMapedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MapedFile mapedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mapedFile = mapedFile;
    }


    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }


    public int getSize() {
        return size;
    }


    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }


    public MapedFile getMapedFile() {
        return mapedFile;
    }


    @Override
    protected void finalize() {
        if (this.mapedFile != null) {
            this.release();
        }
    }

    public synchronized void release() {
        if (this.mapedFile != null) {
            this.mapedFile.release();
            this.mapedFile = null;
        }
    }

    public long getStartOffset() {
        return startOffset;
    }
}
