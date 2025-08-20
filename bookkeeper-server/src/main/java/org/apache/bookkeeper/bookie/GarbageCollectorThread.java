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

import static org.apache.bookkeeper.util.BookKeeperConstants.METADATA_CACHE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.bookie.stats.GarbageCollectorStats;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.ldb.PersistentEntryLogMetadataMap;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
    private static final int SECOND = 1000;
    private static final int ENTRY_LOG_USAGE_SEGMENT_COUNT = 10;
    private static final long MINUTE = TimeUnit.MINUTES.toMillis(1);

    // Maps entry log files to the set of ledgers that comprise the file and the size usage per ledger
    private final EntryLogMetadataMap entryLogMetaMap;

    private final ScheduledExecutorService gcExecutor;
    Future<?> scheduledFuture = null;

    // This is the fixed delay in milliseconds before running the Garbage Collector Thread again.
    final long gcWaitTime;

    // Compaction parameters
    boolean isForceMinorCompactionAllow = false;
    boolean enableMinorCompaction = false;
    final double minorCompactionThreshold;
    final long minorCompactionInterval;
    final long minorCompactionMaxTimeMillis;
    long lastMinorCompactionTime;

    boolean isForceMajorCompactionAllow = false;
    boolean enableMajorCompaction = false;
    final double majorCompactionThreshold;
    final long majorCompactionInterval;
    long majorCompactionMaxTimeMillis;
    long lastMajorCompactionTime;

    final long entryLocationCompactionInterval;
    long randomCompactionDelay;
    long lastEntryLocationCompactionTime;

    @Getter
    final boolean isForceGCAllowWhenNoSpace;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    AbstractLogCompactor compactor;

    // Stats loggers for garbage collection operations
    private final GarbageCollectorStats gcStats;

    private volatile long activeEntryLogSize;
    private volatile long totalEntryLogSize;
    private volatile int numActiveEntryLogs;
    private volatile double entryLogCompactRatio;
    private volatile int[] currentEntryLogUsageBuckets;

    final CompactableLedgerStorage ledgerStorage;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    // use to get the compacting status
    final AtomicBoolean minorCompacting = new AtomicBoolean(false);
    final AtomicBoolean majorCompacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // Boolean to trigger a forced GC.
    final AtomicBoolean forceGarbageCollection = new AtomicBoolean(false);
    // Boolean to disable major compaction, when disk is almost full
    final AtomicBoolean suspendMajorCompaction = new AtomicBoolean(false);
    // Boolean to disable minor compaction, when disk is full
    final AtomicBoolean suspendMinorCompaction = new AtomicBoolean(false);

    final ScanAndCompareGarbageCollector garbageCollector;
    final GarbageCleaner garbageCleaner;

    final ServerConfiguration conf;
    final LedgerDirsManager ledgerDirsManager;

    private static final AtomicLong threadNum = new AtomicLong(0);
    final AbstractLogCompactor.Throttler throttler;

    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf, LedgerManager ledgerManager,
                                  final LedgerDirsManager ledgerDirsManager,
                                  final CompactableLedgerStorage ledgerStorage,
                                  EntryLogger entryLogger,
                                  StatsLogger statsLogger) throws IOException {
        this(conf, ledgerManager, ledgerDirsManager, ledgerStorage, entryLogger, statsLogger, newExecutor());
    }

    @VisibleForTesting
    static ScheduledExecutorService newExecutor() {
        return Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("GarbageCollectorThread"));
    }

    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf,
                                  LedgerManager ledgerManager,
                                  final LedgerDirsManager ledgerDirsManager,
                                  final CompactableLedgerStorage ledgerStorage,
                                  EntryLogger entryLogger,
                                  StatsLogger statsLogger,
                                  ScheduledExecutorService gcExecutor)
        throws IOException {
        this.gcExecutor = gcExecutor;
        this.conf = conf;

        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLogger = entryLogger;
        this.entryLogMetaMap = createEntryLogMetadataMap();
        this.ledgerStorage = ledgerStorage;
        this.gcWaitTime = conf.getGcWaitTime();

        this.numActiveEntryLogs = 0;
        this.activeEntryLogSize = 0L;
        this.totalEntryLogSize = 0L;
        this.entryLogCompactRatio = 0.0;
        this.currentEntryLogUsageBuckets = new int[ENTRY_LOG_USAGE_SEGMENT_COUNT];
        this.garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, ledgerStorage, conf, statsLogger);
        this.gcStats = new GarbageCollectorStats(
            statsLogger,
            () -> numActiveEntryLogs,
            () -> activeEntryLogSize,
            () -> totalEntryLogSize,
            () -> garbageCollector.getNumActiveLedgers(),
            () -> entryLogCompactRatio,
            () -> currentEntryLogUsageBuckets
        );

        this.garbageCleaner = ledgerId -> {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete ledger : " + ledgerId);
                }
                gcStats.getDeletedLedgerCounter().inc();
                ledgerStorage.deleteLedger(ledgerId);
            } catch (IOException e) {
                LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
            }
        };

        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;
        isForceGCAllowWhenNoSpace = conf.getIsForceGCAllowWhenNoSpace();
        majorCompactionMaxTimeMillis = conf.getMajorCompactionMaxTimeMillis();
        minorCompactionMaxTimeMillis = conf.getMinorCompactionMaxTimeMillis();
        entryLocationCompactionInterval = conf.getEntryLocationCompactionInterval() * SECOND;
        if (entryLocationCompactionInterval > 0) {
            randomCompactionDelay = ThreadLocalRandom.current().nextLong(entryLocationCompactionInterval);
        }

        boolean isForceAllowCompaction = conf.isForceAllowCompaction();

        AbstractLogCompactor.LogRemovalListener remover = new AbstractLogCompactor.LogRemovalListener() {
            @Override
            public void removeEntryLog(long logToRemove) {
                try {
                    GarbageCollectorThread.this.removeEntryLog(logToRemove);
                } catch (EntryLogMetadataMapException e) {
                    // Ignore and continue because ledger will not be cleaned up
                    // from entry-logger in this pass and will be taken care in
                    // next schedule task
                    LOG.warn("Failed to remove entry-log metadata {}", logToRemove, e);
                }
            }
        };
        if (conf.getUseTransactionalCompaction()) {
            this.compactor = new TransactionalEntryLogCompactor(conf, entryLogger, ledgerStorage, remover);
        } else {
            this.compactor = new EntryLogCompactor(conf, entryLogger, ledgerStorage, remover);
        }

        this.throttler = new AbstractLogCompactor.Throttler(conf);
        if (minorCompactionInterval > 0 && minorCompactionThreshold > 0) {
            if (minorCompactionThreshold > 1.0d) {
                throw new IOException("Invalid minor compaction threshold "
                                    + minorCompactionThreshold);
            }
            if (minorCompactionInterval < gcWaitTime) {
                throw new IOException("Too short minor compaction interval : "
                                    + minorCompactionInterval);
            }
            enableMinorCompaction = true;
        }

        if (isForceAllowCompaction) {
            if (minorCompactionThreshold > 0 && minorCompactionThreshold < 1.0d) {
                isForceMinorCompactionAllow = true;
            }
            if (majorCompactionThreshold > 0 && majorCompactionThreshold < 1.0d) {
                isForceMajorCompactionAllow = true;
            }
        }

        if (majorCompactionInterval > 0 && majorCompactionThreshold > 0) {
            if (majorCompactionThreshold > 1.0d) {
                throw new IOException("Invalid major compaction threshold "
                                    + majorCompactionThreshold);
            }
            if (majorCompactionInterval < gcWaitTime) {
                throw new IOException("Too short major compaction interval : "
                                    + majorCompactionInterval);
            }
            enableMajorCompaction = true;
        }

        if (enableMinorCompaction && enableMajorCompaction) {
            if (minorCompactionInterval >= majorCompactionInterval
                || minorCompactionThreshold >= majorCompactionThreshold) {
                throw new IOException("Invalid minor/major compaction settings : minor ("
                                    + minorCompactionThreshold + ", " + minorCompactionInterval
                                    + "), major (" + majorCompactionThreshold + ", "
                                    + majorCompactionInterval + ")");
            }
        }

        if (entryLocationCompactionInterval > 0) {
            if (entryLocationCompactionInterval < gcWaitTime) {
                throw new IOException(
                        "Too short entry location compaction interval : " + entryLocationCompactionInterval);
            }
        }

        LOG.info("Minor Compaction : enabled=" + enableMinorCompaction + ", threshold="
               + minorCompactionThreshold + ", interval=" + minorCompactionInterval);
        LOG.info("Major Compaction : enabled=" + enableMajorCompaction + ", threshold="
               + majorCompactionThreshold + ", interval=" + majorCompactionInterval);
        LOG.info("Entry Location Compaction : interval=" + entryLocationCompactionInterval + ", randomCompactionDelay="
                + randomCompactionDelay);

        lastMinorCompactionTime = lastMajorCompactionTime =
            lastEntryLocationCompactionTime = System.currentTimeMillis();
    }

    private EntryLogMetadataMap createEntryLogMetadataMap() throws IOException {
        if (conf.isGcEntryLogMetadataCacheEnabled()) {
            String baseDir = Strings.isNullOrEmpty(conf.getGcEntryLogMetadataCachePath())
                ? this.ledgerDirsManager.getAllLedgerDirs().get(0).getPath() : conf.getGcEntryLogMetadataCachePath();
            try {
                return new PersistentEntryLogMetadataMap(baseDir, conf);
            } catch (IOException e) {
                LOG.error("Failed to initialize persistent-metadata-map , clean up {}",
                    baseDir + "/" + METADATA_CACHE, e);
                throw e;
            }
        } else {
            return new InMemoryEntryLogMetadataMap();
        }
    }

    public void enableForceGC() {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}", Thread.currentThread().getName());
            triggerGC(true, suspendMajorCompaction.get(),
                      suspendMinorCompaction.get());
        }
    }

    public void enableForceGC(boolean forceMajor, boolean forceMinor) {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}, forceMajor: {}, forceMinor: {}",
                Thread.currentThread().getName(), forceMajor, forceMinor);
            triggerGC(true, !forceMajor, !forceMinor);
        }
    }

    public void disableForceGC() {
        if (forceGarbageCollection.compareAndSet(true, false)) {
            LOG.info("{} disabled force garbage collection since bookie has enough space now.", Thread
                    .currentThread().getName());
        }
    }

    Future<?> triggerGC(final boolean force,
                        final boolean suspendMajor,
                        final boolean suspendMinor) {
        return gcExecutor.submit(() -> {
                runWithFlags(force, suspendMajor, suspendMinor);
            });
    }

    Future<?> triggerGC() {
        final boolean force = forceGarbageCollection.get();
        final boolean suspendMajor = suspendMajorCompaction.get();
        final boolean suspendMinor = suspendMinorCompaction.get();

        return gcExecutor.submit(() -> {
                runWithFlags(force, suspendMajor, suspendMinor);
            });
    }

    public boolean isInForceGC() {
        return forceGarbageCollection.get();
    }

    public boolean isMajorGcSuspend() {
        return suspendMajorCompaction.get();
    }

    public boolean isMinorGcSuspend() {
        return suspendMinorCompaction.get();
    }

    public void suspendMajorGC() {
        if (suspendMajorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Major Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMajorGC() {
        if (suspendMajorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Major Compaction back to normal since bookie has enough space now.",
                    Thread.currentThread().getName());
        }
    }

    public void suspendMinorGC() {
        if (suspendMinorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Minor Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMinorGC() {
        if (suspendMinorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Minor Compaction back to normal since bookie has enough space now.",
                    Thread.currentThread().getName());
        }
    }

    public void start() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        long initialDelay = getModInitialDelay();
        scheduledFuture = gcExecutor.scheduleWithFixedDelay(this, initialDelay, gcWaitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * when number of ledger's Dir are more than 1,the same of GarbageCollectorThread will do the same thing,
     * Especially
     * 1) deleting ledger, then SyncThread will be timed to do rocksDB compact
     * 2) compact: entry, cost cpu.
     * then get Mod initial Delay time to simply avoid GarbageCollectorThread working at the same time
     */
    public long getModInitialDelay() {
        int ledgerDirsNum = conf.getLedgerDirs().length;
        long splitTime = gcWaitTime / ledgerDirsNum;
        long currentThreadNum = threadNum.incrementAndGet();
        return gcWaitTime + currentThreadNum * splitTime;
    }

    @Override
    public void run() {
        boolean force = forceGarbageCollection.get();
        boolean suspendMajor = suspendMajorCompaction.get();
        boolean suspendMinor = suspendMinorCompaction.get();

        runWithFlags(force, suspendMajor, suspendMinor);
    }

    /**
     * 执行带有控制标志的垃圾回收线程主入口。
     * @param force 是否强制执行 GC，忽略等待时间
     * @param suspendMajor 是否挂起 major compaction（主压缩）
     * @param suspendMinor 是否挂起 minor compaction（次压缩）
     */
    public void runWithFlags(boolean force, boolean suspendMajor, boolean suspendMinor) {
        // 记录线程启动时刻，用于统计耗时
        long threadStart = MathUtils.nowInNano();

        if (force) {
            LOG.info("Garbage collector thread forced to perform GC before expiry of wait time.");
        }

        // 如果采用事务性压缩，先进行恢复与清理工作，避免前一次异常遗留
        compactor.cleanUpAndRecover();

        try {
            // 1. 回收无效（inactive/deleted）账本对应的 ledger
            //    在 extractMetaFromEntryLogs 中会依赖它来精准计算 entry log 使用占比
            doGcLedgers();

            // 2. 提取 entry log 的元数据信息
            long extractMetaStart = MathUtils.nowInNano();
            try {
                // 解析所有 entry log（不包括仍在写入的当前 log）中承载哪些 ledger 的信息
                extractMetaFromEntryLogs();

                // 回收 entry log 文件，
                // 这里主要是进行统计工作，entry log日志回收逻辑在extractMetaFromEntryLogs已经进行了一遍，
                // 即检查entry log 中的ledger的存活状态，然后删除没有ledger关联的entry log
                doGcEntryLogs();

                // 提取元数据阶段计时统计：成功
                gcStats.getExtractMetaRuntime()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(extractMetaStart), TimeUnit.NANOSECONDS);
            } catch (EntryLogMetadataMapException e) {
                // 提取元数据阶段计时统计：失败
                gcStats.getExtractMetaRuntime()
                        .registerFailedEvent(MathUtils.elapsedNanos(extractMetaStart), TimeUnit.NANOSECONDS);
                throw e;
            }

            // 根据磁盘空间情况，考虑是否需要中止大或小压缩
            if (suspendMajor) {
                LOG.info("Disk almost full, suspend major compaction to slow down filling disk.");
            }
            if (suspendMinor) {
                LOG.info("Disk full, suspend minor compaction to slow down filling disk.");
            }

            // 3. Compaction 部分
            long curTime = System.currentTimeMillis();
            long compactStart = MathUtils.nowInNano();

            // 判断是否允许进行 major compaction（主压缩）
            // 条件：允许强制+force 或配置开启majorCompaction，并满足时间间隔且未挂起
            if (((isForceMajorCompactionAllow && force) ||
                    (enableMajorCompaction && (force || curTime - lastMajorCompactionTime > majorCompactionInterval)))
                    && (!suspendMajor)) {
                // 进入 major compaction（主压缩逻辑）
                LOG.info("Enter major compaction, suspendMajor {}, lastMajorCompactionTime {}", suspendMajor,
                        lastMajorCompactionTime);
                majorCompacting.set(true);
                try {
                    doCompactEntryLogs(majorCompactionThreshold, majorCompactionMaxTimeMillis);
                } catch (EntryLogMetadataMapException e) {
                    // 主压缩失败，统计异常
                    gcStats.getCompactRuntime()
                            .registerFailedEvent(MathUtils.elapsedNanos(compactStart), TimeUnit.NANOSECONDS);
                    throw e;
                } finally {
                    // 刷新主/次压缩时间标记、计数器复位
                    lastMajorCompactionTime = System.currentTimeMillis();
                    lastMinorCompactionTime = lastMajorCompactionTime;
                    gcStats.getMajorCompactionCounter().inc();
                    majorCompacting.set(false);
                }

                // 判断是否允许进行 minor compaction（主压缩）
            } else if (((isForceMinorCompactionAllow && force) ||
                    (enableMinorCompaction && (force || curTime - lastMinorCompactionTime > minorCompactionInterval)))
                    && (!suspendMinor)) {
                // 进入 minor compaction（次压缩逻辑）
                LOG.info("Enter minor compaction, suspendMinor {}, lastMinorCompactionTime {}", suspendMinor,
                        lastMinorCompactionTime);
                minorCompacting.set(true);
                try {
                    doCompactEntryLogs(minorCompactionThreshold, minorCompactionMaxTimeMillis);
                } catch (EntryLogMetadataMapException e) {
                    // 次压缩失败，统计异常
                    gcStats.getCompactRuntime()
                            .registerFailedEvent(MathUtils.elapsedNanos(compactStart), TimeUnit.NANOSECONDS);
                    throw e;
                } finally {
                    lastMinorCompactionTime = System.currentTimeMillis();
                    gcStats.getMinorCompactionCounter().inc();
                    minorCompacting.set(false);
                }
            }

            // 4. entry location compaction ~ 每隔一段时间清理 entry location
            // 条件：配置不为0，且距离上次执行已隔够 interval+随机延迟
            if (entryLocationCompactionInterval > 0 &&
                    (curTime - lastEntryLocationCompactionTime > (entryLocationCompactionInterval + randomCompactionDelay))) {
                LOG.info("Enter entry location compaction, entryLocationCompactionInterval {}, randomCompactionDelay {}, lastEntryLocationCompactionTime {}",
                        entryLocationCompactionInterval, randomCompactionDelay, lastEntryLocationCompactionTime);
                ledgerStorage.entryLocationCompact();
                lastEntryLocationCompactionTime = System.currentTimeMillis();

                // 随机化下一次 compaction 的触发间隔，避免热点
                randomCompactionDelay = ThreadLocalRandom.current().nextLong(entryLocationCompactionInterval);
                LOG.info("Next entry location compaction interval {}", entryLocationCompactionInterval + randomCompactionDelay);

                gcStats.getEntryLocationCompactionCounter().inc();
            }

            // 统计压缩总耗时（只统计成功情况）
            gcStats.getCompactRuntime()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(compactStart), TimeUnit.NANOSECONDS);

            // 统计整个 GC 线程耗时（成功）
            gcStats.getGcThreadRuntime().registerSuccessfulEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);

        } catch (EntryLogMetadataMapException e) {
            // 捕获元数据相关异常，登记失败，并记录日志
            LOG.error("Error in entryLog-metadatamap, Failed to complete GC/Compaction due to entry-log {}", e.getMessage(), e);
            gcStats.getGcThreadRuntime().registerFailedEvent(
                    MathUtils.elapsedNanos(threadStart), TimeUnit.NANOSECONDS);
        } finally {
            // 强制 GC 后将 force 标志重新置为 false，避免漏掉下次强制
            if (force && forceGarbageCollection.compareAndSet(true, false)) {
                LOG.info("{} Set forceGarbageCollection to false after force GC to make it forceGC-able again.",
                        Thread.currentThread().getName());
            }
        }
    }

    /**
     * Do garbage collection ledger index files.
     */
    private void doGcLedgers() {
        long gcLedgersStart = MathUtils.nowInNano();
        try {
            garbageCollector.gc(garbageCleaner);
            gcStats.getGcLedgerRuntime()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(gcLedgersStart), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            LOG.warn("Exception when doing gc ledger.", t);
            gcStats.getGcLedgerRuntime()
                    .registerFailedEvent(MathUtils.elapsedNanos(gcLedgersStart), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * 垃圾回收不再关联任何活动 ledger 的 entry log 文件。
     */
    private void doGcEntryLogs() throws EntryLogMetadataMapException {
        // 统计活跃的 entry log 大小和所有 entry log 的总大小，直到本轮 GC 完成后再统一更新
        AtomicLong activeEntryLogSizeAcc = new AtomicLong(0L);
        AtomicLong totalEntryLogSizeAcc = new AtomicLong(0L);

        // 遍历所有 entry log 文件的元数据，清理已经没有被任何活动 ledger 所引用的文件
        entryLogMetaMap.forEach((entryLogId, meta) -> {
            try {
                // 移除 entry log 文件中不存在的 ledger，返回是否有修改
                boolean modified = removeIfLedgerNotExists(meta);
                if (meta.isEmpty()) {
                    // 如果 entry log 文件已不再关联任何活动 ledger
                    // 可以安全地删除该 entry log 文件，释放磁盘空间
                    LOG.info("删除 entryLogId {}，因为其没有任何活动 ledger！", entryLogId);
                    if (removeEntryLog(entryLogId)) {
                        // 删除文件成功，累计被回收的空间
                        gcStats.getReclaimedSpaceViaDeletes().addCount(meta.getTotalSize());
                    } else {
                        // 删除失败，累计失败次数
                        gcStats.getReclaimFailedToDelete().inc();
                    }
                } else if (modified) {
                    // 只有当 meta 被修改时才更新 entryLogMetaMap，减少不必要的写操作
                    entryLogMetaMap.put(meta.getEntryLogId(), meta);
                }
            } catch (EntryLogMetadataMapException e) {
                // 某些 ledger 删除过程中可能异常，跳过并继续处理下一个 entry log
                // 未清理干净的 ledger 会在下次定时任务中再次尝试清理
                LOG.warn("从 entry log 元数据中移除 ledger 失败，entryLogId: {}", entryLogId, e);
            }
            // 累计当前 entry log 文件内还关联的 ledger 的剩余数据大小
            activeEntryLogSizeAcc.getAndAdd(meta.getRemainingSize());
            // 累计所有 entry log 文件的总大小
            totalEntryLogSizeAcc.getAndAdd(meta.getTotalSize());
        });
        // 将累计结果赋值为当前对象的统计属性
        this.activeEntryLogSize = activeEntryLogSizeAcc.get();
        this.totalEntryLogSize = totalEntryLogSizeAcc.get();
        this.numActiveEntryLogs = entryLogMetaMap.size();
    }


    private boolean removeIfLedgerNotExists(EntryLogMetadata meta) throws EntryLogMetadataMapException {
        MutableBoolean modified = new MutableBoolean(false);
        meta.removeLedgerIf((entryLogLedger) -> {
            // Remove the entry log ledger from the set if it isn't active.
            try {
                boolean exist = ledgerStorage.ledgerExists(entryLogLedger);
                if (!exist) {
                    modified.setTrue();
                }
                return !exist;
            } catch (IOException e) {
                LOG.error("Error reading from ledger storage", e);
                return false;
            }
        });

        return modified.getValue();
    }

    /**
     * 根据需要对 entry log 文件进行压缩 (Compaction)。
     *
     * <p>
     * 压缩会从 low remaining size percentage 到 high remaining size percentage 顺序执行。
     * 也就是说，优先 compact 剩余有效数据较少的 entry log 文件。
     * 那些 remaining size percentage（即 entry log 文件中，仍然在使用的 ledger 数据的总大小，
     * 占 entry log 文件总大小的比例）高于设置的 threshold 的 entry log 文件不会被 compact。
     * </p>
     *
     * @param threshold          剩余有效数据比例（remaining size percentage）阈值。只 compact 低于该阈值的 entry log。
     * @param maxTimeMillis      本次压缩操作的最大运行时长，超时自动中止。
     * @throws EntryLogMetadataMapException
     */
    @VisibleForTesting
    void doCompactEntryLogs(double threshold, long maxTimeMillis) throws EntryLogMetadataMapException {
        LOG.info("Do compaction to compact those files lower than {}", threshold);

        // 按 usage（remaining size percentage，有效数据比例）分桶做统计
        final int numBuckets = ENTRY_LOG_USAGE_SEGMENT_COUNT;  // 分段个数
        int[] entryLogUsageBuckets = new int[numBuckets];       // 每个桶内 entry log 文件计数
        int[] compactedBuckets = new int[numBuckets];           // 每个桶实际 compact 的 entry log 文件计数

        // 每个桶存 entry log id 链表，这些 log 是可 compact 的
        ArrayList<LinkedList<Long>> compactableBuckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            compactableBuckets.add(new LinkedList<>());
        }

        long start = System.currentTimeMillis();
        MutableLong end = new MutableLong(start);
        MutableLong timeDiff = new MutableLong(0);

        // 遍历所有 entry log 文件
        entryLogMetaMap.forEach((entryLogId, meta) -> {
            // 获取 entry log 的 remaining size percentage
            double usage = meta.getUsage();
            if (conf.isUseTargetEntryLogSizeForGc() && usage < 1.0d) {
                usage = (double) meta.getRemainingSize() / Math.max(meta.getTotalSize(), conf.getEntryLogSizeLimit());
            }
            // 根据有效数据比例分段归入桶
            int bucketIndex = calculateUsageIndex(numBuckets, usage);
            entryLogUsageBuckets[bucketIndex]++;

            // 计时，判断是否已经超时
            if (timeDiff.getValue() < maxTimeMillis) {
                end.setValue(System.currentTimeMillis());
                timeDiff.setValue(end.getValue() - start);
            }
            // 满足下列其一则不需要 compact：
            // 1. usage 高于阈值（数据太多了，不值得搬迁）；
            // 2. 已经超时；
            // 3. 当前 GC 任务已停止。
            if ((usage >= threshold
                    || (maxTimeMillis > 0 && timeDiff.getValue() >= maxTimeMillis)
                    || !running)) {
                return;
            }
            // 加入待 compact 列表
            compactableBuckets.get(bucketIndex).add(meta.getEntryLogId());
        });

        // 保留当前所有桶（分段）的统计信息，便于后续运维和监控
        currentEntryLogUsageBuckets = entryLogUsageBuckets;
        gcStats.setEntryLogUsageBuckets(currentEntryLogUsageBuckets);

        LOG.info(
                "Compaction: entry log usage buckets before compaction [10% 20% 30% 40% 50% 60% 70% 80% 90% 100%] = {}",
                entryLogUsageBuckets);

        // 只对低于 threshold 的桶做压缩
        final int maxBucket = calculateUsageIndex(numBuckets, threshold);

        // 统计即将要 compact 的 entry log 文件总数
        int totalEntryLogIds = 0;
        for (int currBucket = 0; currBucket <= maxBucket; currBucket++) {
            totalEntryLogIds += compactableBuckets.get(currBucket).size();
        }
        long lastPrintTimestamp = 0;
        AtomicInteger processedEntryLogCnt = new AtomicInteger(0);

        // 进入压缩主循环，从低到高逐桶处理
        stopCompaction:
        for (int currBucket = 0; currBucket <= maxBucket; currBucket++) {
            LinkedList<Long> entryLogIds = compactableBuckets.get(currBucket);
            while (!entryLogIds.isEmpty()) {
                // 计时，判超时
                if (timeDiff.getValue() < maxTimeMillis) {
                    end.setValue(System.currentTimeMillis());
                    timeDiff.setValue(end.getValue() - start);
                }

                // 已超时，或系统要求终止压缩，直接跳出
                if ((maxTimeMillis > 0 && timeDiff.getValue() >= maxTimeMillis) || !running) {
                    break stopCompaction;
                }

                final int bucketIndex = currBucket;
                final long logId = entryLogIds.remove();

                // 每分钟打印一次进度日志
                if (System.currentTimeMillis() - lastPrintTimestamp >= MINUTE) {
                    lastPrintTimestamp = System.currentTimeMillis();
                    LOG.info("Compaction progress {} / {}, current compaction entryLogId: {}",
                            processedEntryLogCnt.get(), totalEntryLogIds, logId);
                }
                // 实际压缩对应的 entry log
                entryLogMetaMap.forKey(logId, (entryLogId, meta) -> {
                    if (meta == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Metadata for entry log {} already deleted", logId);
                        }
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Compacting entry log {} with usage {} below threshold {}",
                                meta.getEntryLogId(), meta.getUsage(), threshold);
                    }

                    long priorRemainingSize = meta.getRemainingSize();
                    compactEntryLog(meta); // 实际搬迁/compact逻辑
                    gcStats.getReclaimedSpaceViaCompaction().addCount(meta.getTotalSize() - priorRemainingSize);

                    compactedBuckets[bucketIndex]++;
                    processedEntryLogCnt.getAndIncrement();
                });
            }
        }

        // 打印调试日志，辅助定位调度、超时等原因
        if (LOG.isDebugEnabled()) {
            if (!running) {
                LOG.debug("Compaction exited due to gc not running");
            }
            if (maxTimeMillis > 0 && timeDiff.getValue() > maxTimeMillis) {
                LOG.debug("Compaction ran for {}ms but was limited by {}ms", timeDiff, maxTimeMillis);
            }
        }
        // 统计压缩比，便于监控聚合指标
        int totalEntryLogNum = Arrays.stream(entryLogUsageBuckets).sum();
        int compactedEntryLogNum = Arrays.stream(compactedBuckets).sum();
        this.entryLogCompactRatio = totalEntryLogNum == 0 ? 0 : (double) compactedEntryLogNum / totalEntryLogNum;

        LOG.info("Compaction: entry log usage buckets[10% 20% 30% 40% 50% 60% 70% 80% 90% 100%] = {}, compacted {}, "
                + "compacted entry log ratio {}", entryLogUsageBuckets, compactedBuckets, entryLogCompactRatio);
    }


    /**
     * 根据 entry log 的 usage（remaining size percentage）计算应该归属于哪个桶（bucket）的索引。
     * usage 范围在 0.0 到 1.0 之间，表示 entry log 文件中正在被 ledger 使用的数据比例（remaining size percentage）。
     *
     * 例如：如果 numBuckets = 10，则每个桶覆盖 10% 的 usage 区间，
     * usage 在 0.0 ~ 0.099 映射到第0桶，0.10~0.199 映射到第1桶... 0.9~1.0 映射到第9桶。
     *
     * @param numBuckets 分桶（bucket）总数
     * @param usage usage（remaining size percentage），值域为 0.0 - 1.0，表示 entry log 的有效数据比例
     * @return 归属桶的索引（index），范围 0 ~ numBuckets-1。即使 usage=1.0，也属于最后一个桶。
     */
    int calculateUsageIndex(int numBuckets, double usage) {
        // 例如，usage=0.57，numBuckets=10，则 index=Math.floor(0.57*10)=5，表示第5号桶
        // 如果 usage=1.0，则 index=Math.floor(10)=10，但结果限制最大为9（即 numBuckets-1）
        return Math.min(
                numBuckets - 1,
                (int) Math.floor(usage * numBuckets));
    }

    /**
     * Shutdown the garbage collector thread.
     *
     * @throws InterruptedException if there is an exception stopping gc thread.
     */
    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    public synchronized void shutdown() throws InterruptedException {
        if (!this.running) {
            return;
        }
        this.running = false;
        LOG.info("Shutting down GarbageCollectorThread");

        throttler.cancelledAcquire();
        compactor.throttler.cancelledAcquire();
        while (!compacting.compareAndSet(false, true)) {
            // Wait till the thread stops compacting
            Thread.sleep(100);
        }

        // Interrupt GC executor thread
        gcExecutor.shutdownNow();
        try {
            entryLogMetaMap.close();
        } catch (Exception e) {
            LOG.warn("Failed to close entryLog metadata-map", e);
        }
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     * @throws EntryLogMetadataMapException
     */
    protected boolean removeEntryLog(long entryLogId) throws EntryLogMetadataMapException {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing entry log metadata for {}", entryLogId);
            entryLogMetaMap.remove(entryLogId);
            return true;
        }

        return false;
    }

    /**
     * 对单个 entry log 文件进行数据压缩（compaction）。
     * 对象 entryLogMeta 表示该 entry log 的元数据，其中包含剩余有效数据比例（remaining size percentage）。
     *
     * 压缩过程将有效数据迁移到新的 entry log，以便清理掉无效数据，提升空间利用率。
     *
     * 实现机制：
     * - 压缩期间打上 compacting 标志，防止过程中被 shutdown（比如实例关闭），否则可能出现 ClosedByInterruptException，
     *   这可能导致索引文件和 entry logger 同时关闭甚至损坏。
     * - 如果当前 log 已在压缩（compacting 为 true），则直接返回，避免重复压缩。
     * - 操作结束后，及时清除 compacting 标志。
     *
     * @param entryLogMeta  需要被压缩的 entry log 元数据信息，包含剩余有效数据比例等属性
     */
    protected void compactEntryLog(EntryLogMetadata entryLogMeta) {
        // 类似 Sync Thread 的处理
        // 尝试将 compacting 标志设为 true，表示进入压缩期间，防止在压缩过程中被关停
        // 否则 Shutdown 时收到 ClosedByInterruptException，可能导致索引或 entry logger 文件损坏
        if (!compacting.compareAndSet(false, true)) {
            // 设置 compacting 标志失败，说明该 EntryLogId 的压缩已在进行，直接返回
            return;
        }

        try {
            // 调用 compactor 执行实际的压缩逻辑：迁移剩余有效 ledger 数据
            compactor.compact(entryLogMeta);
        } catch (Exception e) {
            LOG.error("Failed to compact entry log {} due to unexpected error", entryLogMeta.getEntryLogId(), e);
        } finally {
            // 压缩完成，重置 compacting 标志
            compacting.set(false);
        }
    }

    /**
     * 读取所有尚未处理的 entry log 文件，并分析每个 entry log 文件中包含的 ledger ID 集合。
     *
     * @throws EntryLogMetadataMapException
     */
    protected void extractMetaFromEntryLogs() throws EntryLogMetadataMapException {
        // 遍历所有已经刷新（写入磁盘）的 entry log 文件的 ID
        for (long entryLogId : entryLogger.getFlushedLogIds()) {
            // 如果当前 entry log 文件的元数据已经被提取过则跳过，避免重复处理
            if (entryLogMetaMap.containsKey(entryLogId)) {
                continue;
            }

            // 检查 entry log 文件是否存在
            // 若不存在，文件可能已被垃圾回收（GC）删除，直接跳过
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            try {
                // 读取 entry log 文件并提取元数据信息（包括该文件中的 ledger ID 信息）
                EntryLogMetadata entryLogMeta = entryLogger.getEntryLogMetadata(entryLogId, throttler);
                LOG.info("已提取 entry log 元数据：entryLogId: {}, 含有的 ledgers: {}",
                        entryLogId, entryLogMeta.getLedgersMap().keys());

                // 更新entryLogMeta中的ledger信息，移除那些不再存在的ledger
                removeIfLedgerNotExists(entryLogMeta);
                if (entryLogMeta.isEmpty()) {
                    // entry log 文件未关联任何活动 ledger，说明所有 ledger 都已删除或失效
                    // 可以安全删除该 entry log 文件以节省磁盘空间
                    LOG.info("删除 entryLogId {} 因其已无活动 ledger!", entryLogId);
                    if (removeEntryLog(entryLogId)) {
                        // 删除成功，统计被回收的空间
                        gcStats.getReclaimedSpaceViaDeletes().addCount(entryLogMeta.getTotalSize());
                    } else {
                        // 删除失败，统计删除失败次数
                        gcStats.getReclaimFailedToDelete().inc();
                    }
                } else {
                    // entry log 仍有关联的活动 ledger，保存其元数据到缓存
                    entryLogMetaMap.put(entryLogId, entryLogMeta);
                }
            } catch (IOException | RuntimeException e) {
                // 处理过程中异常（如 IO 错误、运行时异常），记录日志，剩余处理由恢复进程完成
                LOG.warn("处理 {} 时产生异常，恢复流程将处理该问题", entryLogId, e);
            } catch (OutOfMemoryError oome) {
                // 处理 entry log 文件时遇到内存溢出（常因文件损坏导致 entry size 异常大），记录日志并跳过该文件
                LOG.warn("处理 {} 时出现 OutOfMemoryError，跳过该 entry log", entryLogId, oome);
            }
        }
    }


    CompactableLedgerStorage getLedgerStorage() {
        return ledgerStorage;
    }

    @VisibleForTesting
    EntryLogMetadataMap getEntryLogMetaMap() {
        return entryLogMetaMap;
    }

    public GarbageCollectionStatus getGarbageCollectionStatus() {
        return GarbageCollectionStatus.builder()
            .forceCompacting(forceGarbageCollection.get())
            .majorCompacting(majorCompacting.get())
            .minorCompacting(minorCompacting.get())
            .lastMajorCompactionTime(lastMajorCompactionTime)
            .lastMinorCompactionTime(lastMinorCompactionTime)
            .lastEntryLocationCompactionTime(lastEntryLocationCompactionTime)
            .majorCompactionCounter(gcStats.getMajorCompactionCounter().get())
            .minorCompactionCounter(gcStats.getMinorCompactionCounter().get())
            .entryLocationCompactionCounter(gcStats.getEntryLocationCompactionCounter().get())
            .build();
    }
}
