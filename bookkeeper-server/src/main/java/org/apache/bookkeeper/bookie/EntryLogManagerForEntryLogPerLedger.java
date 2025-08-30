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

package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ENTRYLOGGER_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ENTRYLOGS_PER_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NUM_OF_WRITE_ACTIVE_LEDGERS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.lang3.mutable.MutableInt;

@Slf4j
class EntryLogManagerForEntryLogPerLedger extends EntryLogManagerBase {

    static class BufferedLogChannelWithDirInfo {
        private final BufferedLogChannel logChannel;
        volatile boolean ledgerDirFull = false;

        private BufferedLogChannelWithDirInfo(BufferedLogChannel logChannel) {
            this.logChannel = logChannel;
        }

        private boolean isLedgerDirFull() {
            return ledgerDirFull;
        }

        private void setLedgerDirFull(boolean ledgerDirFull) {
            this.ledgerDirFull = ledgerDirFull;
        }

        BufferedLogChannel getLogChannel() {
            return logChannel;
        }
    }

    class EntryLogAndLockTuple {
        private final Lock ledgerLock;
        private BufferedLogChannelWithDirInfo entryLogWithDirInfo;

        private EntryLogAndLockTuple(long ledgerId) {
            int lockIndex = MathUtils.signSafeMod(Long.hashCode(ledgerId), lockArrayPool.length());
            if (lockArrayPool.get(lockIndex) == null) {
                lockArrayPool.compareAndSet(lockIndex, null, new ReentrantLock());
            }
            ledgerLock = lockArrayPool.get(lockIndex);
        }

        private Lock getLedgerLock() {
            return ledgerLock;
        }

        BufferedLogChannelWithDirInfo getEntryLogWithDirInfo() {
            return entryLogWithDirInfo;
        }

        private void setEntryLogWithDirInfo(BufferedLogChannelWithDirInfo entryLogWithDirInfo) {
            this.entryLogWithDirInfo = entryLogWithDirInfo;
        }
    }

    @StatsDoc(
        name = ENTRYLOGGER_SCOPE,
        category = CATEGORY_SERVER,
        help = "EntryLogger related stats"
    )
    class EntryLogsPerLedgerCounter {

        @StatsDoc(
            name = NUM_OF_WRITE_ACTIVE_LEDGERS,
            help = "Number of write active ledgers"
        )
        private final Counter numOfWriteActiveLedgers;
        @StatsDoc(
            name = NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY,
            help = "Number of write ledgers removed after cache expiry"
        )
        private final Counter numOfWriteLedgersRemovedCacheExpiry;
        @StatsDoc(
            name = NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE,
            help = "Number of write ledgers removed due to reach max cache size"
        )
        private final Counter numOfWriteLedgersRemovedCacheMaxSize;
        @StatsDoc(
            name = NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS,
            help = "Number of ledgers having multiple entry logs"
        )
        private final Counter numLedgersHavingMultipleEntrylogs;
        @StatsDoc(
            name = ENTRYLOGS_PER_LEDGER,
            help = "The distribution of number of entry logs per ledger"
        )
        private final OpStatsLogger entryLogsPerLedger;
        /*
         * ledgerIdEntryLogCounterCacheMap cache will be used to store count of
         * entrylogs as value for its ledgerid key. This cacheMap limits -
         * 'expiry duration' and 'maximumSize' will be set to
         * entryLogPerLedgerCounterLimitsMultFactor times of
         * 'ledgerIdEntryLogMap' cache limits. This is needed because entries
         * from 'ledgerIdEntryLogMap' can be removed from cache because of
         * accesstime expiry or cache size limits, but to know the actual number
         * of entrylogs per ledger, we should maintain this count for long time.
         */
        private final LoadingCache<Long, MutableInt> ledgerIdEntryLogCounterCacheMap;

        EntryLogsPerLedgerCounter(StatsLogger statsLogger) {
            this.numOfWriteActiveLedgers = statsLogger.getCounter(NUM_OF_WRITE_ACTIVE_LEDGERS);
            this.numOfWriteLedgersRemovedCacheExpiry = statsLogger
                    .getCounter(NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_EXPIRY);
            this.numOfWriteLedgersRemovedCacheMaxSize = statsLogger
                    .getCounter(NUM_OF_WRITE_LEDGERS_REMOVED_CACHE_MAXSIZE);
            this.numLedgersHavingMultipleEntrylogs = statsLogger.getCounter(NUM_LEDGERS_HAVING_MULTIPLE_ENTRYLOGS);
            this.entryLogsPerLedger = statsLogger.getOpStatsLogger(ENTRYLOGS_PER_LEDGER);

            ledgerIdEntryLogCounterCacheMap = CacheBuilder.newBuilder()
                    .expireAfterAccess(entrylogMapAccessExpiryTimeInSeconds * entryLogPerLedgerCounterLimitsMultFactor,
                            TimeUnit.SECONDS)
                    .maximumSize(maximumNumberOfActiveEntryLogs * entryLogPerLedgerCounterLimitsMultFactor)
                    .removalListener(new RemovalListener<Long, MutableInt>() {
                        @Override
                        public void onRemoval(RemovalNotification<Long, MutableInt> removedEntryFromCounterMap) {
                            if ((removedEntryFromCounterMap != null)
                                    && (removedEntryFromCounterMap.getValue() != null)) {
                                synchronized (EntryLogsPerLedgerCounter.this) {
                                    entryLogsPerLedger
                                            .registerSuccessfulValue(removedEntryFromCounterMap.getValue().intValue());
                                }
                            }
                        }
                    }).build(new CacheLoader<Long, MutableInt>() {
                        @Override
                        public MutableInt load(Long key) throws Exception {
                            synchronized (EntryLogsPerLedgerCounter.this) {
                                return new MutableInt();
                            }
                        }
                    });
        }

        private synchronized void openNewEntryLogForLedger(Long ledgerId, boolean newLedgerInEntryLogMapCache) {
            int numOfEntrylogsForThisLedger = ledgerIdEntryLogCounterCacheMap.getUnchecked(ledgerId).incrementAndGet();
            if (numOfEntrylogsForThisLedger == 2) {
                numLedgersHavingMultipleEntrylogs.inc();
            }
            if (newLedgerInEntryLogMapCache) {
                numOfWriteActiveLedgers.inc();
            }
        }

        private synchronized void removedLedgerFromEntryLogMapCache(Long ledgerId, RemovalCause cause) {
            numOfWriteActiveLedgers.dec();
            if (cause.equals(RemovalCause.EXPIRED)) {
                numOfWriteLedgersRemovedCacheExpiry.inc();
            } else if (cause.equals(RemovalCause.SIZE)) {
                numOfWriteLedgersRemovedCacheMaxSize.inc();
            }
        }

        /*
         * this is for testing purpose only. guava's cache doesnt cleanup
         * completely (including calling expiry removal listener) automatically
         * when access timeout elapses.
         *
         * https://google.github.io/guava/releases/19.0/api/docs/com/google/
         * common/cache/CacheBuilder.html
         *
         * If expireAfterWrite or expireAfterAccess is requested entries may be
         * evicted on each cache modification, on occasional cache accesses, or
         * on calls to Cache.cleanUp(). Expired entries may be counted by
         * Cache.size(), but will never be visible to read or write operations.
         *
         * Certain cache configurations will result in the accrual of periodic
         * maintenance tasks which will be performed during write operations, or
         * during occasional read operations in the absence of writes. The
         * Cache.cleanUp() method of the returned cache will also perform
         * maintenance, but calling it should not be necessary with a high
         * throughput cache. Only caches built with removalListener,
         * expireAfterWrite, expireAfterAccess, weakKeys, weakValues, or
         * softValues perform periodic maintenance.
         */
        @VisibleForTesting
        void doCounterMapCleanup() {
            ledgerIdEntryLogCounterCacheMap.cleanUp();
        }

        @VisibleForTesting
        ConcurrentMap<Long, MutableInt> getCounterMap() {
            return ledgerIdEntryLogCounterCacheMap.asMap();
        }
    }

    private final AtomicReferenceArray<Lock> lockArrayPool;
    private final LoadingCache<Long, EntryLogAndLockTuple> ledgerIdEntryLogMap;
    /*
     * every time active logChannel is accessed from ledgerIdEntryLogMap
     * cache, the accesstime of that entry is updated. But for certain
     * operations we dont want to impact accessTime of the entries (like
     * periodic flush of current active logChannels), and those operations
     * can use this copy of references.
     */
    private final ConcurrentLongHashMap<BufferedLogChannelWithDirInfo> replicaOfCurrentLogChannels;
    private final CacheLoader<Long, EntryLogAndLockTuple> entryLogAndLockTupleCacheLoader;
    private final DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    private final int entrylogMapAccessExpiryTimeInSeconds;
    private final int maximumNumberOfActiveEntryLogs;
    private final int entryLogPerLedgerCounterLimitsMultFactor;

    // Expose Stats
    private final StatsLogger statsLogger;
    final EntryLogsPerLedgerCounter entryLogsPerLedgerCounter;

    // EntryLogManagerForEntryLogPerLedger 构造函数
    EntryLogManagerForEntryLogPerLedger(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
                                        EntryLoggerAllocator entryLoggerAllocator,
                                        List<DefaultEntryLogger.EntryLogListener> listeners,
                                        DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus,
                                        StatsLogger statsLogger) throws IOException {
        // 调用父类构造函数，初始化基础属性
        super(conf, ledgerDirsManager, entryLoggerAllocator, listeners);

        // 初始化最近创建的 EntryLog 状态
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
        // 旋转日志通道列表（支持并发，写时复制）
        this.rotatedLogChannels = new CopyOnWriteArrayList<BufferedLogChannel>();
        // 当前日志通道副本（根据 ledgerId 字段查找，支持并发）
        this.replicaOfCurrentLogChannels =
                ConcurrentLongHashMap.<BufferedLogChannelWithDirInfo>newBuilder().build();
        // 访问 entrylogMap 过期时间（秒）
        this.entrylogMapAccessExpiryTimeInSeconds = conf.getEntrylogMapAccessExpiryTimeInSeconds();
        // 活动 EntryLog 的最大数量限制
        this.maximumNumberOfActiveEntryLogs = conf.getMaximumNumberOfActiveEntryLogs();
        // ledger 计数器限制倍率因子
        this.entryLogPerLedgerCounterLimitsMultFactor = conf.getEntryLogPerLedgerCounterLimitsMultFactor();

        // 注册 LedgerDirsListener 目录变更监听
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        // 锁数组池，大小为最大活动 EntryLog 的两倍
        this.lockArrayPool = new AtomicReferenceArray<Lock>(maximumNumberOfActiveEntryLogs * 2);

        // 定义 CacheLoader 加载器，按 ledgerId 加载 EntryLogAndLockTuple 对象
        this.entryLogAndLockTupleCacheLoader = new CacheLoader<Long, EntryLogAndLockTuple>() {
            @Override
            public EntryLogAndLockTuple load(Long key) throws Exception {
                return new EntryLogAndLockTuple(key);
            }
        };

        /*
         * 当前依赖于访问时间驱逐策略自动移除 EntryLogAndLockTuple，
         * 如果某 ledger 的 EntryLogAndLockTuple 在 entrylogMapAccessExpiryTimeInSeconds 时间内未被访问，则会自动从缓存移除。
         *
         * 未来会引入显式的 advisory writeClose 调用，届时可以明确地从缓存移除 Tuple。
         * 但仍需保留基于时间的策略，以应对异常情况下不能收到 write close 调用的状况。
         */
        ledgerIdEntryLogMap = CacheBuilder.newBuilder()
                // 设置访问过期时间：entrylogMapAccessExpiryTimeInSeconds 秒
                .expireAfterAccess(entrylogMapAccessExpiryTimeInSeconds, TimeUnit.SECONDS)
                // 设置缓存最大大小：maximumNumberOfActiveEntryLogs
                .maximumSize(maximumNumberOfActiveEntryLogs)
                // 移除监听器：Tuple 被移除时触发 onCacheEntryRemoval 方法
                .removalListener(new RemovalListener<Long, EntryLogAndLockTuple>() {
                    @Override
                    public void onRemoval(
                            RemovalNotification<Long, EntryLogAndLockTuple> expiredLedgerEntryLogMapEntry) {
                        onCacheEntryRemoval(expiredLedgerEntryLogMapEntry);
                    }
                })
                // 加载器（上述定义的 CacheLoader）
                .build(entryLogAndLockTupleCacheLoader);

        // 统计日志对象
        this.statsLogger = statsLogger;
        // ledger 维度的 entry log 计数器
        this.entryLogsPerLedgerCounter = new EntryLogsPerLedgerCounter(this.statsLogger);
    }


    /*
     * This method is called when an entry is removed from the cache. This could
     * be because access time of that ledger has elapsed
     * entrylogMapAccessExpiryTimeInSeconds period, or number of active
     * currentlogs in the cache has reached the size of
     * maximumNumberOfActiveEntryLogs, or if an entry is explicitly
     * invalidated/removed. In these cases entry for that ledger is removed from
     * cache. Since the entrylog of this ledger is not active anymore it has to
     * be removed from replicaOfCurrentLogChannels and added to
     * rotatedLogChannels.
     *
     * Because of performance/optimizations concerns the cleanup maintenance
     * operations wont happen automatically, for more info on eviction cleanup
     * maintenance tasks -
     * https://google.github.io/guava/releases/19.0/api/docs/com/google/
     * common/cache/CacheBuilder.html
     *
     */
    private void onCacheEntryRemoval(RemovalNotification<Long, EntryLogAndLockTuple> removedLedgerEntryLogMapEntry) {
        Long ledgerId = removedLedgerEntryLogMapEntry.getKey();
        if (log.isDebugEnabled()) {
            log.debug("LedgerId {} is being evicted from the cache map because of {}", ledgerId,
                    removedLedgerEntryLogMapEntry.getCause());
        }
        EntryLogAndLockTuple entryLogAndLockTuple = removedLedgerEntryLogMapEntry.getValue();
        if (entryLogAndLockTuple == null) {
            log.error("entryLogAndLockTuple is not supposed to be null in entry removal listener for ledger : {}",
                    ledgerId);
            return;
        }
        Lock lock = entryLogAndLockTuple.ledgerLock;
        BufferedLogChannelWithDirInfo logChannelWithDirInfo = entryLogAndLockTuple.getEntryLogWithDirInfo();
        if (logChannelWithDirInfo == null) {
            log.error("logChannel for ledger: {} is not supposed to be null in entry removal listener", ledgerId);
            return;
        }
        lock.lock();
        try {
            BufferedLogChannel logChannel = logChannelWithDirInfo.getLogChannel();
            // Append ledgers map at the end of entry log
            try {
                logChannel.appendLedgersMap();
            } catch (Exception e) {
                log.error("Got IOException while trying to appendLedgersMap in cacheEntryRemoval callback", e);
            }
            replicaOfCurrentLogChannels.remove(logChannel.getLogId());
            rotatedLogChannels.add(logChannel);
            entryLogsPerLedgerCounter.removedLedgerFromEntryLogMapCache(ledgerId,
                    removedLedgerEntryLogMapEntry.getCause());
        } finally {
            lock.unlock();
        }
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = getCopyOfCurrentLogs();
                for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
                    if (disk.equals(currentLogWithDirInfo.getLogChannel().getLogFile().getParentFile())) {
                        currentLogWithDirInfo.setLedgerDirFull(true);
                    }
                }
            }

            @Override
            public void diskWritable(File disk) {
                Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = getCopyOfCurrentLogs();
                for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
                    if (disk.equals(currentLogWithDirInfo.getLogChannel().getLogFile().getParentFile())) {
                        currentLogWithDirInfo.setLedgerDirFull(false);
                    }
                }
            }
        };
    }

    Lock getLock(long ledgerId) throws IOException {
        try {
            return ledgerIdEntryLogMap.get(ledgerId).getLedgerLock();
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching lock to acquire for ledger: " + ledgerId, e);
            throw new IOException("Received unexpected exception while fetching lock to acquire", e);
        }
    }

    /*
     * sets the logChannel for the given ledgerId. It will add the new
     * logchannel to replicaOfCurrentLogChannels, and the previous one will
     * be removed from replicaOfCurrentLogChannels. Previous logChannel will
     * be added to rotatedLogChannels in both the cases.
     */
    @Override
    public void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel) throws IOException {
        Lock lock = getLock(ledgerId);
        lock.lock();
        try {
            BufferedLogChannel hasToRotateLogChannel = getCurrentLogForLedger(ledgerId);
            boolean newLedgerInEntryLogMapCache = (hasToRotateLogChannel == null);
            logChannel.setLedgerIdAssigned(ledgerId);
            BufferedLogChannelWithDirInfo logChannelWithDirInfo = new BufferedLogChannelWithDirInfo(logChannel);
            ledgerIdEntryLogMap.get(ledgerId).setEntryLogWithDirInfo(logChannelWithDirInfo);
            entryLogsPerLedgerCounter.openNewEntryLogForLedger(ledgerId, newLedgerInEntryLogMapCache);
            replicaOfCurrentLogChannels.put(logChannel.getLogId(), logChannelWithDirInfo);
            if (hasToRotateLogChannel != null) {
                replicaOfCurrentLogChannels.remove(hasToRotateLogChannel.getLogId());
                rotatedLogChannels.add(hasToRotateLogChannel);
            }
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching entry from map for ledger: " + ledgerId, e);
            throw new IOException("Received unexpected exception while fetching entry from map", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public BufferedLogChannel getCurrentLogForLedger(long ledgerId) throws IOException {
        BufferedLogChannelWithDirInfo bufferedLogChannelWithDirInfo = getCurrentLogWithDirInfoForLedger(ledgerId);
        BufferedLogChannel bufferedLogChannel = null;
        if (bufferedLogChannelWithDirInfo != null) {
            bufferedLogChannel = bufferedLogChannelWithDirInfo.getLogChannel();
        }
        return bufferedLogChannel;
    }

    public BufferedLogChannelWithDirInfo getCurrentLogWithDirInfoForLedger(long ledgerId) throws IOException {
        Lock lock = getLock(ledgerId);
        lock.lock();
        try {
            EntryLogAndLockTuple entryLogAndLockTuple = ledgerIdEntryLogMap.get(ledgerId);
            return entryLogAndLockTuple.getEntryLogWithDirInfo();
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching entry from map for ledger: " + ledgerId, e);
            throw new IOException("Received unexpected exception while fetching entry from map", e);
        } finally {
            lock.unlock();
        }
    }

    public Set<BufferedLogChannelWithDirInfo> getCopyOfCurrentLogs() {
        return new HashSet<BufferedLogChannelWithDirInfo>(replicaOfCurrentLogChannels.values());
    }

    @Override
    public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
        BufferedLogChannelWithDirInfo bufferedLogChannelWithDirInfo = replicaOfCurrentLogChannels.get(entryLogId);
        BufferedLogChannel logChannel = null;
        if (bufferedLogChannelWithDirInfo != null) {
            logChannel = bufferedLogChannelWithDirInfo.getLogChannel();
        }
        return logChannel;
    }

    @Override
    public void checkpoint() throws IOException {
        /*
         * In the case of entryLogPerLedgerEnabled we need to flush
         * both rotatedlogs and currentlogs. This is needed because
         * syncThread periodically does checkpoint and at this time
         * all the logs should be flushed.
         *
         */
        super.flush();
    }

    @Override
    public void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException {
        // do nothing
        /*
         * prepareSortedLedgerStorageCheckpoint is required for
         * singleentrylog scenario, but it is not needed for
         * entrylogperledger scenario, since entries of a ledger go
         * to a entrylog (even during compaction) and SyncThread
         * drives periodic checkpoint logic.
         */

    }

    @Override
    public void prepareEntryMemTableFlush() {
        // do nothing
    }

    @Override
    public boolean commitEntryMemTableFlush() throws IOException {
        // lock it only if there is new data
        // so that cache accesstime is not changed
        Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = getCopyOfCurrentLogs();
        for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            BufferedLogChannel currentLog = currentLogWithDirInfo.getLogChannel();
            if (reachEntryLogLimit(currentLog, 0L)) {
                Long ledgerId = currentLog.getLedgerIdAssigned();
                Lock lock = getLock(ledgerId);
                lock.lock();
                try {
                    if (reachEntryLogLimit(currentLog, 0L)) {
                        log.info("Rolling entry logger since it reached size limitation for ledger: {}", ledgerId);
                        createNewLog(ledgerId, "after entry log file is rotated");
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
        /*
         * in the case of entrylogperledger, SyncThread drives
         * checkpoint logic for every flushInterval. So
         * EntryMemtable doesn't need to call checkpoint in the case
         * of entrylogperledger.
         */
        return false;
    }

    /*
     * this is for testing purpose only. guava's cache doesnt cleanup
     * completely (including calling expiry removal listener) automatically
     * when access timeout elapses.
     *
     * https://google.github.io/guava/releases/19.0/api/docs/com/google/
     * common/cache/CacheBuilder.html
     *
     * If expireAfterWrite or expireAfterAccess is requested entries may be
     * evicted on each cache modification, on occasional cache accesses, or
     * on calls to Cache.cleanUp(). Expired entries may be counted by
     * Cache.size(), but will never be visible to read or write operations.
     *
     * Certain cache configurations will result in the accrual of periodic
     * maintenance tasks which will be performed during write operations, or
     * during occasional read operations in the absence of writes. The
     * Cache.cleanUp() method of the returned cache will also perform
     * maintenance, but calling it should not be necessary with a high
     * throughput cache. Only caches built with removalListener,
     * expireAfterWrite, expireAfterAccess, weakKeys, weakValues, or
     * softValues perform periodic maintenance.
     */
    @VisibleForTesting
    void doEntryLogMapCleanup() {
        ledgerIdEntryLogMap.cleanUp();
    }

    @VisibleForTesting
    ConcurrentMap<Long, EntryLogAndLockTuple> getCacheAsMap() {
        return ledgerIdEntryLogMap.asMap();
    }
    /*
     * 返回拥有当前活跃 entrylog 数量最少的可写 ledger 目录
     */
    @Override
    public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
        // 创建一个映射，记录每个可写 ledger 目录当前活跃 entrylog 的数量，初始为0
        Map<File, MutableInt> writableLedgerDirFrequency = new HashMap<File, MutableInt>();
        writableLedgerDirs.stream()
                .forEach((ledgerDir) -> writableLedgerDirFrequency.put(ledgerDir, new MutableInt()));

        // 遍历所有当前日志通道，统计每个目录中的活跃日志数
        for (BufferedLogChannelWithDirInfo logChannelWithDirInfo : replicaOfCurrentLogChannels.values()) {
            // 获取当前日志通道文件的父目录
            File parentDirOfCurrentLogChannel = logChannelWithDirInfo.getLogChannel().getLogFile().getParentFile();
            if (writableLedgerDirFrequency.containsKey(parentDirOfCurrentLogChannel)) {
                // 如果该目录可写，则该目录的日志数+1
                writableLedgerDirFrequency.get(parentDirOfCurrentLogChannel).increment();
            }
        }

        // 在所有可写目录中找到拥有当前活跃 entrylog 最少的目录
        @SuppressWarnings("unchecked")
        Optional<Entry<File, MutableInt>> ledgerDirWithLeastNumofCurrentLogs = writableLedgerDirFrequency.entrySet()
                .stream().min(Map.Entry.comparingByValue());

        // 返回拥有最少日志数的目录
        return ledgerDirWithLeastNumofCurrentLogs.get().getKey();
    }

    @Override
    public void close() throws IOException {
        Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = getCopyOfCurrentLogs();
        for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            if (currentLogWithDirInfo.getLogChannel() != null) {
                currentLogWithDirInfo.getLogChannel().close();
            }
        }
    }

    @Override
    public void forceClose() {
        Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = getCopyOfCurrentLogs();
        for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            IOUtils.close(log, currentLogWithDirInfo.getLogChannel());
        }
    }

    @Override
    void flushCurrentLogs() throws IOException {
        Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = getCopyOfCurrentLogs();
        for (BufferedLogChannelWithDirInfo logChannelWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            /**
             * flushCurrentLogs method is called during checkpoint, so metadata
             * of the file also should be force written.
             */
            flushLogChannel(logChannelWithDirInfo.getLogChannel(), true);
        }
    }

    @Override
    public BufferedLogChannel createNewLogForCompaction() throws IOException {
        throw new UnsupportedOperationException(
                "When entryLogPerLedger is enabled, transactional compaction should have been disabled");
    }

    @Override
    public long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        Lock lock = getLock(ledger);
        lock.lock();
        try {
            return super.addEntry(ledger, entry, rollLog);
        } finally {
            lock.unlock();
        }
    }

    @Override
    void createNewLog(long ledgerId) throws IOException {
        Lock lock = getLock(ledgerId);
        lock.lock();
        try {
            super.createNewLog(ledgerId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize, boolean rollLog)
            throws IOException {
        // 获取与指定 ledgerId 相关的锁
        Lock lock = getLock(ledgerId);
        lock.lock();
        try {
            // 获取当前 Ledger 对应的日志通道以及其目录信息
            BufferedLogChannelWithDirInfo logChannelWithDirInfo = getCurrentLogWithDirInfoForLedger(ledgerId);
            BufferedLogChannel logChannel = null;
            if (logChannelWithDirInfo != null) {
                // 若日志通道信息不为空，则获取具体的日志通道
                logChannel = logChannelWithDirInfo.getLogChannel();
            }
            // 检查是否到达 entry log 限制
            // rollLog 为 true 时检测软限制，否则检测硬限制
            boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(logChannel, entrySize)
                    : readEntryLogHardLimit(logChannel, entrySize);
            // 若日志大小达到限制或当前磁盘已满则创建新日志
            boolean diskFull = (logChannel == null) ? false : logChannelWithDirInfo.isLedgerDirFull();
            // 检查是否所有磁盘都不可写
            boolean allDisksFull = !ledgerDirsManager.hasWritableLedgerDirs();

            /**
             * 逻辑说明：
             * 如果当前日志通道的磁盘已满，或者日志通道达到大小限制，或者日志通道尚未初始化，
             * 则需要创建新日志。
             * 如果所有磁盘都已满（Bookie进入只读模式），就继续使用当前日志通道，
             * 这种情况下 addEntry 操作只可能是 GC（垃圾回收），可以继续写入。
             */
            if ((diskFull && (!allDisksFull)) || reachEntryLogLimit || (logChannel == null)) {
                if (logChannel != null) {
                    // 如果当前日志通道不为空，则进行冲刷并强制写盘
                    logChannel.flushAndForceWriteIfRegularFlush(false);
                }
                // 创建新的日志通道
                createNewLog(ledgerId,
                        ": diskFull = " + diskFull + ", allDisksFull = " + allDisksFull
                                + ", reachEntryLogLimit = " + reachEntryLogLimit + ", logChannel = " + logChannel);
            }

            // 返回当前 ledger 的日志通道（可能是新创建的）
            return getCurrentLogForLedger(ledgerId);
        } finally {
            // 解锁
            lock.unlock();
        }
    }


    @Override
    public void flushRotatedLogs() throws IOException {
        for (BufferedLogChannel channel : rotatedLogChannels) {
            channel.flushAndForceWrite(true);
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            channel.close();
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            rotatedLogChannels.remove(channel);
            log.info("Synced entry logger {} to disk.", channel.getLogId());
        }
    }
}
