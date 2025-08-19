/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Write cache implementation. 写缓存实现类
 *
 * <p>将指定大小的缓存空间从堆外(DirectMemory)分配，并拆分为多段(segment)管理
 * <p>所有entry被追加到同一个buffer中, entry通过hashmap索引
 * <p>支持按 (ledgerId, entryId) 迭代遍历已存 entry，保证顺序性
 */
public class WriteCache implements Closeable {

    /**
     * 用于遍历WriteCache的entry的回调接口
     */
    public interface EntryConsumer {
        void  accept(long ledgerId, long entryId, ByteBuf entry) throws IOException;
    }

    // 用于索引每个entry的位置和长度：key为(ledgerId, entryId)；value为(offset, size)
    private final ConcurrentLongLongPairHashMap index = ConcurrentLongLongPairHashMap.newBuilder()
            .expectedItems(4096)
            .concurrencyLevel(2 * Runtime.getRuntime().availableProcessors())
            .build();

    // 每个ledger当前最新entryId映射，用于乱序写入场景
    private final ConcurrentLongLongHashMap lastEntryMap = ConcurrentLongLongHashMap.newBuilder()
            .expectedItems(4096)
            .concurrencyLevel(2 * Runtime.getRuntime().availableProcessors())
            .build();

    // 分段的缓存，每段一个ByteBuf，按顺序分配
    private final ByteBuf[] cacheSegments;
    // 总段数
    private final int segmentsCount;

    // 最大写缓存总容量
    private final long maxCacheSize;
    // 单个缓存段最大容量
    private final int maxSegmentSize;
    // 段内偏移掩码，用于计算某entry在段内位置
    private final long segmentOffsetMask;
    // 段号计算所需移位量
    private final long segmentOffsetBits;

    // 当前缓存已用总字节数
    private final AtomicLong cacheSize = new AtomicLong(0);
    // 全局分配偏移量（每次插入entry时原子递增，以分配空间）
    private final AtomicLong cacheOffset = new AtomicLong(0);
    // 记录缓存中entry数量
    private final LongAdder cacheCount = new LongAdder();

    // 标记哪些ledger被删除了，不再参与迭代
    private final ConcurrentLongHashSet deletedLedgers = ConcurrentLongHashSet.newBuilder().build();

    // Netty的内存分配器（用于新建临时ByteBuf如get()方法）
    private final ByteBufAllocator allocator;

    // 构造器：默认单段最大为1GB
    public WriteCache(ByteBufAllocator allocator, long maxCacheSize) {
        this(allocator, maxCacheSize, 1 * 1024 * 1024 * 1024);
    }

    /**
     * 核心构造函数
     * 分配maxCacheSize大小的缓存空间，切分为若干个maxSegmentSize的段
     */
    public WriteCache(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize) {
        checkArgument(maxSegmentSize > 0);

        // 强制段大小必须是2^n
        long alignedMaxSegmentSize = alignToPowerOfTwo(maxSegmentSize);
        checkArgument(maxSegmentSize == alignedMaxSegmentSize, "Max segment size needs to be in form of 2^n");

        this.allocator = allocator;
        this.maxCacheSize = maxCacheSize;
        this.maxSegmentSize = (int) maxSegmentSize;
        this.segmentOffsetMask = maxSegmentSize - 1;  // 用于快速取段内偏移
        this.segmentOffsetBits = 63 - Long.numberOfLeadingZeros(maxSegmentSize); // 计算分段所需移位

        // 计算段数：cacheSize可能不是maxSegmentSize整数倍，多出来的空间分配最后一段
        this.segmentsCount = 1 + (int) (maxCacheSize / maxSegmentSize);

        this.cacheSegments = new ByteBuf[segmentsCount];

        for (int i = 0; i < segmentsCount - 1; i++) {
            // 前面的段分配满容量
            cacheSegments[i] = Unpooled.directBuffer(maxSegmentSize, maxSegmentSize);
        }

        // 最后一段只分配剩余空间（可能小于maxSegmentSize）
        int lastSegmentSize = (int) (maxCacheSize % maxSegmentSize);
        cacheSegments[segmentsCount - 1] = Unpooled.directBuffer(lastSegmentSize, lastSegmentSize);
    }

    /**
     * 清空缓存及所有索引
     */
    public void clear() {
        cacheSize.set(0L);
        cacheOffset.set(0L);
        cacheCount.reset();
        index.clear();
        lastEntryMap.clear();
        deletedLedgers.clear();
    }

    /**
     * 关闭并释放所有buffer资源
     */
    @Override
    public void close() {
        for (ByteBuf buf : cacheSegments) {
            buf.release();
        }
    }

    /**
     * 写入一个 entry 到缓存
     * @return true 写入成功; false 缓存满无法写入
     */
    public boolean put(long ledgerId, long entryId, ByteBuf entry) {
        int size = entry.readableBytes();

        // 64字节对齐，防止L1缓存行争用，提高多线程并发性能
        int alignedSize = align64(size);

        long offset;
        int localOffset;
        int segmentIdx;

        while (true) {
            // 原子分配全局写入偏移，保证多线程安全
            offset = cacheOffset.getAndAdd(alignedSize);

            // 计算段内偏移
            localOffset = (int) (offset & segmentOffsetMask);
            // 计算写入的是哪个段
            segmentIdx = (int) (offset >>> segmentOffsetBits);

            if ((offset + size) > maxCacheSize) {
                // 已无空间
                log.warn("缓存已满，无法插入 ledgerId={} entryId={}, entrySize={}", ledgerId, entryId, size);
                return false;
            } else if (maxSegmentSize - localOffset < size) {
                // 当前段内剩余空间不足，需重试下一个可用段
                log.debug("当前段剩余空间不足，重试下一个段 ledgerId={} entryId={}, entrySize={}", ledgerId, entryId, size);
                continue;
            } else {
                // 找到可写空间
                log.debug("分配缓冲区 offset={} segmentIdx={} localOffset={}, ledgerId={} entryId={}, entrySize={}", offset, segmentIdx, localOffset, ledgerId, entryId, size);
                break;
            }
        }

        // 把entry内容写入计算好的段的偏移处
        cacheSegments[segmentIdx].setBytes(localOffset, entry, entry.readerIndex(), entry.readableBytes());

        // 根据ledgerId更新"最后写入的entryId"，保证落盘顺序/便于后续快速定位
        while (true) {
            long currentLastEntryId = lastEntryMap.get(ledgerId);
            if (currentLastEntryId > entryId) {
                // 已存在更新的entryId，无需覆盖
                log.debug("lastEntryMap 已存在较新 entryId，跳过覆盖 ledgerId={}, currentLastEntryId={}, entryId={}", ledgerId, currentLastEntryId, entryId);
                break;
            }

            if (lastEntryMap.compareAndSet(ledgerId, currentLastEntryId, entryId)) {
                // 成功写入
                log.debug("lastEntryMap 写入 ledgerId={}, entryId={}，原值={}", ledgerId, entryId, currentLastEntryId);
                break;
            }
            log.debug("lastEntryMap CAS失败，重试 ledgerId={} entryId={}", ledgerId, entryId);
        }

        // 记录索引 (ledgerId, entryId) -> (offset, size)，方便查找
        index.put(ledgerId, entryId, offset, size);
        cacheCount.increment();
        cacheSize.addAndGet(size);

        log.info("成功插入缓存 ledgerId={} entryId={} offset={} size={}", ledgerId, entryId, offset, size);
        return true;
    }

    /**
     * 读取指定 ledgerId/entryId 的 entry内容副本
     * @return 不存在则返回null; 否则返回一个新的缓存副本
     */
    public ByteBuf get(long ledgerId, long entryId) {
        LongPair result = index.get(ledgerId, entryId);
        if (result == null) {
            return null;
        }

        long offset = result.first;
        int size = (int) result.second;

        // 新建一个副本拷贝
        ByteBuf entry = allocator.buffer(size, size);

        // 定位在段中的偏移
        int localOffset = (int) (offset & segmentOffsetMask);
        int segmentIdx = (int) (offset >>> segmentOffsetBits);
        entry.writeBytes(cacheSegments[segmentIdx], localOffset, size);

        return entry;
    }

    /** 是否存在对应entry */
    public boolean hasEntry(long ledgerId, long entryId) {
        return index.get(ledgerId, entryId) != null;
    }

    /** 获取ledger最新entry */
    public ByteBuf getLastEntry(long ledgerId) {
        long lastEntryId = lastEntryMap.get(ledgerId);
        if (lastEntryId == -1) {
            return null;
        } else {
            return get(ledgerId, lastEntryId);
        }
    }

    /** 逻辑删除ledger（后续遍历将忽略它） */
    public void deleteLedger(long ledgerId) {
        deletedLedgers.add(ledgerId);
    }

    /**
     * 遍历所有未删除ledger的entry，按(ledgerId, entryId)排序后调用EntryConsumer
     * 若缓存很大，可能消耗较多内存与时间，建议仅在确需此顺序遍历时使用
     */
    public void forEach(EntryConsumer consumer) throws IOException {
        sortedEntriesLock.lock();
        try {
            int entriesToSort = (int) index.size();
            int arrayLen = entriesToSort * 4;
            if (sortedEntries == null || sortedEntries.length < arrayLen) {
                // 每个entry用4个long存储: ledgerId, entryId, offset, length
                sortedEntries = new long[(int) (arrayLen * 2)];
            }

            long startTime = MathUtils.nowInNano();

            sortedEntriesIdx = 0;
            // 将所有未删除ledger的索引信息收集并写入临时数组
            index.forEach((ledgerId, entryId, offset, length) -> {
                if (deletedLedgers.contains(ledgerId)) {
                    // 忽略已删除ledger
                    return;
                }
                sortedEntries[sortedEntriesIdx] = ledgerId;
                sortedEntries[sortedEntriesIdx + 1] = entryId;
                sortedEntries[sortedEntriesIdx + 2] = offset;
                sortedEntries[sortedEntriesIdx + 3] = length;
                sortedEntriesIdx += 4;
            });

            if (log.isDebugEnabled()) {
                log.debug("iteration took {} ms", MathUtils.elapsedNanos(startTime) / 1e6);
            }

            startTime = MathUtils.nowInNano();

            // 按ledgerId, entryId排序, 保持每组4个元素为一个'entry'的结构关系
            ArrayGroupSort.sort(sortedEntries, 0, sortedEntriesIdx);

            if (log.isDebugEnabled()) {
                log.debug("sorting {} ms", (MathUtils.elapsedNanos(startTime) / 1e6));
            }
            startTime = MathUtils.nowInNano();

            // 为后续读取准备分片副本（slice减少副本产生）
            ByteBuf[] entrySegments = new ByteBuf[segmentsCount];
            for (int i = 0; i < segmentsCount; i++) {
                entrySegments[i] = cacheSegments[i].slice(0, cacheSegments[i].capacity());
            }

            // 遍历排序后的entries，回调外部consumer即可
            for (int i = 0; i < sortedEntriesIdx; i += 4) {
                long ledgerId = sortedEntries[i];
                long entryId = sortedEntries[i + 1];
                long offset = sortedEntries[i + 2];
                long length = sortedEntries[i + 3];

                int localOffset = (int) (offset & segmentOffsetMask);
                int segmentIdx = (int) (offset >>> segmentOffsetBits);
                ByteBuf entry = entrySegments[segmentIdx];

                entry.setIndex(localOffset, localOffset + (int) length); // 指定一个视图区间
                consumer.accept(ledgerId, entryId, entry);
            }

            if (log.isDebugEnabled()) {
                log.debug("entry log adding {} ms", MathUtils.elapsedNanos(startTime) / 1e6);
            }
        } finally {
            sortedEntriesLock.unlock();
        }
    }

    /** 当前已用字节数 */
    public long size() {
        return cacheSize.get();
    }

    /** entry总数量 */
    public long count() {
        return cacheCount.sum();
    }

    /** 是否为空 */
    public boolean isEmpty() {
        return cacheSize.get() == 0L;
    }

    // 以下为对齐相关方法 --------------------------------------------

    private static final int ALIGN_64_MASK = ~(64 - 1);

    /**
     * 向上对齐到64字节
     */
    static int align64(int size) {
        return (size + 64 - 1) & ALIGN_64_MASK;
    }

    /**
     * 对齐到不小于n的2的幂
     */
    private static long alignToPowerOfTwo(long n) {
        return (long) Math.pow(2, 64 - Long.numberOfLeadingZeros(n - 1));
    }

    // forEach排序用到的临时状态
    private final ReentrantLock sortedEntriesLock = new ReentrantLock();
    private long[] sortedEntries;
    private int sortedEntriesIdx;

    // 日志
    private static final Logger log = LoggerFactory.getLogger(WriteCache.class);
}
