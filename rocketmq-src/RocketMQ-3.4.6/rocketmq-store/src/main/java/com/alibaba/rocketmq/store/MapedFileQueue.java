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

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *MapedFileQueue：包含了很多MapedFile，以及每个MapedFile的真实大小；
 MapedFile：包含了具体的文件信息，包括文件路径，文件名，文件起始偏移，写位移，读位移等等信息，同事使用了虚拟内存映射来提高IO效率；
 * @author shijia.wxr
 * 无论CommitLog，还是 ConsumeQueue，都有一个对应的MappedFileQueue，也就是对应的内存映射文件的链表
 * 读写时，根据offset定位到链表中，对应的MappedFile，进行读写。通过MappedFile，就很好的解决了大文件随机读的性能问题。
 */
public class MapedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.StoreErrorLoggerName);
    private static final int DeleteFilesBatchMax = 10;
    private final String storePath;
    //commitlog 默认为1024 * 1024 * 1024   consumequeue默认为30w*20
    private final int mapedFileSize;
    //consumequeue目录下的各个存储commitlog的文件信息都加如该list中，见MapedFileQueue
    //文件名即是消息在此文件的中初始偏移量，排好序后组成了一个连续的消息队
    private final List<MapedFile> mapedFiles = new ArrayList<MapedFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final AllocateMapedFileService allocateMapedFileService;
    private long committedWhere = 0;
    private volatile long storeTimestamp = 0;

    /***
     *
     * @param storePath 内存映射文件的存储路径。
     * @param mapedFileSize n内存映射文件的大小 ，
     * @param allocateMapedFileService 分配内存映射文件的服务。
     */
    public MapedFileQueue(final String storePath, int mapedFileSize,
            AllocateMapedFileService allocateMapedFileService) {
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.allocateMapedFileService = allocateMapedFileService;
    }


    public void checkSelf() {
        this.readWriteLock.readLock().lock();
        try {
            if (!this.mapedFiles.isEmpty()) {
                MapedFile first = this.mapedFiles.get(0);
                MapedFile last = this.mapedFiles.get(this.mapedFiles.size() - 1);

                int sizeCompute =
                        (int) ((last.getFileFromOffset() - first.getFileFromOffset()) / this.mapedFileSize) + 1;
                int sizeReal = this.mapedFiles.size();
                if (sizeCompute != sizeReal) {
                    logError
                        .error(
                            "[BUG]The mapedfile queue's data is damaged, {} mapedFileSize={} sizeCompute={} sizeReal={}\n{}", //
                            this.storePath,//
                            this.mapedFileSize,//
                            sizeCompute,//
                            sizeReal,//
                            this.mapedFiles.toString()//
                        );
                }
            }
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public MapedFile getMapedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MapedFile mapedFile = (MapedFile) mfs[i];
            if (mapedFile.getLastModifiedTimestamp() >= timestamp) {
                return mapedFile;
            }
        }

        return (MapedFile) mfs[mfs.length - 1];
    }


    private Object[] copyMapedFiles(final int reservedMapedFiles) {
        Object[] mfs = null;

        try {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.size() <= reservedMapedFiles) {
                return null;
            }

            mfs = this.mapedFiles.toArray();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MapedFile> willRemoveFiles = new ArrayList<MapedFile>();

        for (MapedFile file : this.mapedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mapedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePostion((int) (offset % this.mapedFileSize));
                    file.setCommittedPosition((int) (offset % this.mapedFileSize));
                }
                else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    private void deleteExpiredFile(List<MapedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MapedFile file : files) {
                    if (!this.mapedFiles.remove(file)) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            }
            catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
            finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }


    /**
     [root@s10-2-x-5 0]# ls
     00000000000048000000  00000000000054000000  00000000000060000000  00000000000066000000  00000000000072000000  00000000000078000000  00000000000084000000  00000000000090000000  00000000000096000000  00000000000102000000
     [root@s10-2-x-5 0]# du -sh *
     5.8M    00000000000048000000
     5.8M    00000000000054000000
     5.8M    00000000000060000000
     5.8M    00000000000066000000
     5.8M    00000000000072000000
     5.8M    00000000000078000000
     5.8M    00000000000084000000
     5.8M    00000000000090000000
     5.8M    00000000000096000000
     3.6M    00000000000102000000
     *  把磁盘文件加载到内存。
     * @return  CommitLog.load中执行
     */
    public boolean load() {
        File dir = new File(this.storePath); //commitlog路径/root/store/commitlog 这里面存的是消费记录，记录消息是否被消费等信息
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files); //对consume文件名(也就是offset偏移)排序
            for (File file : files) {
                //??????? 上面的 consumequeue中 00000000000102000000也还没到3.6M，不也忽略了吗？
                if (file.length() != this.mapedFileSize) { //有文件不符合预设的mapfile大小 ，直接忽略掉后面的文件。
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    MapedFile mapedFile = new MapedFile(file.getPath(), mapedFileSize);
                    //文件大小就是mapfile写入和flush的位置。
                    mapedFile.setWrotePostion(this.mapedFileSize);
                    mapedFile.setCommittedPosition(this.mapedFileSize);
                    this.mapedFiles.add(mapedFile);
                    log.info("load " + file.getPath() + " OK");
                }
                catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mapedFiles.isEmpty())
            return 0;

        long committed = this.committedWhere;
        if (committed != 0) {
            MapedFile mapedFile = this.getLastMapedFile(0, false);
            if (mapedFile != null) {
                return (mapedFile.getFileFromOffset() + mapedFile.getWrotePostion()) - committed;
            }
        }

        return 0;
    }


    /**
     * 起始位点从0 开始创建一个Mapfile .
     * @return
     */
    public MapedFile getLastMapedFile() {
        return this.getLastMapedFile(0);
    }

    /**
     * 用读锁获取最后一个Mapfile ..
     * 没有mapfile 则返回null .
     * @return
     */
    public MapedFile getLastMapedFileWithLock() {
        MapedFile mapedFileLast = null;
        this.readWriteLock.readLock().lock();
        if (!this.mapedFiles.isEmpty()) { // 已经有mapfile.  则取最后一个。
            mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() -  1);
        }
        this.readWriteLock.readLock().unlock();

        return mapedFileLast;
    }


    public MapedFile getLastMapedFile(final long startOffset) {
        return getLastMapedFile(startOffset, true);
    }

    /**
     * 是否以创建新的MapFile的方式获取一个新的文件。
     * @param startOffset 开始的起始位点，
     * @param needCreate 是否需要创建新的mapfile .
     * 当消息到达broker时，需要获取最新的MapedFile写入数据，调用MapedFileQueue的getLastMapedFile获取，此函数如果集合中一个
     * 也没有创建一个，如果最后一个写满了也创建一个新的。
     * @return
     */
    public MapedFile getLastMapedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MapedFile mapedFileLast = null;
        {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.isEmpty()) { // mapfile为空的情况，则从startOffset做为create offset .
                createOffset = startOffset - (startOffset % this.mapedFileSize);
            }
            else {  //拿到了最后一个mapfile .
                mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
            }
            this.readWriteLock.readLock().unlock();
        }

        if (mapedFileLast != null && mapedFileLast.isFull()) { //拿到了最后一个mapfile 并且已经写满,  那么Createoffset
            // 就是最后一个mapfile的文件名（offset + 文件大小） 。
            createOffset = mapedFileLast.getFileFromOffset() + this.mapedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            //创建下一个mapfile和下下个mapfile的路径。
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath =
                    this.storePath + File.separator
                            + UtilAll.offset2FileName(createOffset + this.mapedFileSize);
            MapedFile mapedFile = null;

            if (this.allocateMapedFileService != null) { //有异步分配Mapfile的服务。
                mapedFile =
                        this.allocateMapedFileService.putRequestAndReturnMapedFile(nextFilePath,
                            nextNextFilePath, this.mapedFileSize);
            }
            else {//  没有异步分配Mapfile的服务， 就创建一个。
                try {
                    mapedFile = new MapedFile(nextFilePath, this.mapedFileSize);
                }
                catch (IOException e) {
                    log.error("create mapedfile exception", e);
                }
            }

            if (mapedFile != null) { //加写锁，加入到mapfile队列。
                this.readWriteLock.writeLock().lock();
                if (this.mapedFiles.isEmpty()) {
                    mapedFile.setFirstCreateInQueue(true);
                }
                this.mapedFiles.add(mapedFile);
                this.readWriteLock.writeLock().unlock();
            }

            return mapedFile;
        }

        return mapedFileLast;
    }

    public long getMinOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                return this.mapedFiles.get(0).getFileFromOffset();
            }
        }
        catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }

        return -1;
    }


    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                int lastIndex = this.mapedFiles.size() - 1;
                MapedFile mapedFile = this.mapedFiles.get(lastIndex);
                return mapedFile.getFileFromOffset() + mapedFile.getWrotePostion();
            }
        }
        catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }

    public void deleteLastMapedFile() {
        if (!this.mapedFiles.isEmpty()) {
            int lastIndex = this.mapedFiles.size() - 1;
            MapedFile mapedFile = this.mapedFiles.get(lastIndex);
            mapedFile.destroy(1000);
            this.mapedFiles.remove(mapedFile);
            log.info("on recover, destroy a logic maped file " + mapedFile.getFileName());
        }
    }


    public int deleteExpiredFileByTime(//
            final long expiredTime, //
            final int deleteFilesInterval, //
            final long intervalForcibly,//
            final boolean cleanImmediately//
    ) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MapedFile> files = new ArrayList<MapedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MapedFile mapedFile = (MapedFile) mfs[i];
                long liveMaxTimestamp = mapedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp//
                        || cleanImmediately) { //文件超过过期时间或者磁盘空间超过设定阈值（默认75%） ，需要开始启动文件清理。
                    if (mapedFile.destroy(intervalForcibly)) { //清理文件时， 如果文件还在被线程引用， 则通过intervalForcibly
                        //参数控制下一次真正删除文件还要间隔多长时间，默认120s。
                        files.add(mapedFile);
                        deleteCount++;

                        if (files.size() >= DeleteFilesBatchMax) { //一次批量删除的文件数不超过10
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) { //两个mapfile删除操作间间隔100ms .
                            try {
                                Thread.sleep(deleteFilesInterval);
                            }
                            catch (InterruptedException e) {
                            }
                        }
                    }
                    else {
                        break;
                    }
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMapedFiles(0);

        List<MapedFile> files = new ArrayList<MapedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = true;
                MapedFile mapedFile = (MapedFile) mfs[i];
                //找到消费队列的最后一个item。
                SelectMapedBufferResult result = mapedFile.selectMapedBuffer(this.mapedFileSize - unitSize);
                if (result != null) {
                    //消费队列最后一个item指向的commitlog的物理位点比 offset标明的commitlog物理位点还小，说明这个消费队列已经没用了。可以删除。
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = (maxOffsetInLogicQueue < offset);
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mapedfile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                }
                else {
                    log.warn("this being not excuted forever.");
                    break;
                }

                if (destroy && mapedFile.destroy(1000 * 60)) {
                    files.add(mapedFile);
                    deleteCount++;
                }
                else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 一次最少刷几个分页.
     * @param flushLeastPages
     * @return
     */
    public boolean commit(final int flushLeastPages) {
        boolean result = true;
        //按commitlog已经写入的位点找到mapfile .并且在没有找到时， 得到第一个Mapfile。
        MapedFile mapedFile = this.findMapedFileByOffset(this.committedWhere, true);
        if (mapedFile != null) {
            long tmpTimeStamp = mapedFile.getStoreTimestamp();
            int offset = mapedFile.commit(flushLeastPages);
            //commit做刷盘处理以后，where就变成了已经刷盘到哪儿的位置。
            long where = mapedFile.getFileFromOffset() + offset;
            result = (where == this.committedWhere);
            this.committedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }


    /**
     * 和第一个Mapfile起始物理位点的差值除以mapfile文件大小,就得到了当前要写入的mapfile.
     * @param offset
     * @param returnFirstOnNotFound
     * @return
     */  //查找offset在那个MapedFile中
    public MapedFile findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MapedFile mapedFile = this.getFirstMapedFile();

            if (mapedFile != null) {
                int index =
                        (int) ((offset / this.mapedFileSize) - (mapedFile.getFileFromOffset() / this.mapedFileSize));
                if (index < 0 || index >= this.mapedFiles.size()) {
                    logError
                        .warn(
                            "findMapedFileByOffset offset not matched, request Offset: {}, index: {}, mapedFileSize: {}, mapedFiles count: {}, StackTrace: {}",//
                            offset,//
                            index,//
                            this.mapedFileSize,//
                            this.mapedFiles.size(),//
                            UtilAll.currentStackTrace());
                }

                try {
                    return this.mapedFiles.get(index);
                }
                catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mapedFile;
                    }
                }
            }
        }
        catch (Exception e) {
            log.error("findMapedFileByOffset Exception", e);
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    private MapedFile getFirstMapedFile() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }

        return this.mapedFiles.get(0);
    }


    public MapedFile getLastMapedFile2() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }
        return this.mapedFiles.get(this.mapedFiles.size() - 1);
    }


    public MapedFile findMapedFileByOffset(final long offset) {
        return findMapedFileByOffset(offset, false);
    }


    public long getMapedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMapedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mapedFileSize;
                }
            }
        }

        return size;
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MapedFile mapedFile = this.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            if (!mapedFile.isAvailable()) {
                log.warn("the mapedfile was destroyed once, but still alive, " + mapedFile.getFileName());
                boolean result = mapedFile.destroy(intervalForcibly);
                if (result) {
                    log.warn("the mapedfile redelete OK, " + mapedFile.getFileName());
                    List<MapedFile> tmps = new ArrayList<MapedFile>();
                    tmps.add(mapedFile);
                    this.deleteExpiredFile(tmps);
                }
                else {
                    log.warn("the mapedfile redelete Failed, " + mapedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }


    public MapedFile getFirstMapedFileOnLock() {
        try {
            this.readWriteLock.readLock().lock();
            return this.getFirstMapedFile();
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public void shutdown(final long intervalForcibly) {
        this.readWriteLock.readLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            mf.shutdown(intervalForcibly);
        }
        this.readWriteLock.readLock().unlock();
    }


    public void destroy() {
        this.readWriteLock.writeLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mapedFiles.clear();
        this.committedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
        this.readWriteLock.writeLock().unlock();
    }


    public long getCommittedWhere() {
        return committedWhere;
    }


    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<MapedFile> getMapedFiles() {
        return mapedFiles;
    }


    public int getMapedFileSize() {
        return mapedFileSize;
    }
}
