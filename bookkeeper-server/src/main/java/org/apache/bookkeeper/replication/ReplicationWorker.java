/*
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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_NOT_ADHERING_PLACEMENT_LEDGERS_REPLICATED;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATE_EXCEPTION;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REREPLICATE_OP;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.bookie.BookieThread;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplicationWorker will take the fragments one by one from
 * ZKLedgerUnderreplicationManager and replicates to it.
 */
@StatsDoc(
    name = REPLICATION_WORKER_SCOPE,
    help = "replication worker related stats"
)
public class ReplicationWorker implements Runnable {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReplicationWorker.class);
    private static final int REPLICATED_FAILED_LEDGERS_MAXSIZE = 2000;
    public static final int NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS = 5;

    private final LedgerUnderreplicationManager underreplicationManager;
    private final ServerConfiguration conf;
    private volatile boolean workerRunning = false;
    private final BookKeeperAdmin admin;
    private final LedgerChecker ledgerChecker;
    private final BookKeeper bkc;
    private final boolean ownBkc;
    private final Thread workerThread;
    private final long rwRereplicateBackoffMs;
    private final long openLedgerRereplicationGracePeriod;
    private final Timer pendingReplicationTimer;
    private final long lockReleaseOfFailedLedgerGracePeriod;
    private final long baseBackoffForLockReleaseOfFailedLedger;
    private final BiConsumer<Long, Long> onReadEntryFailureCallback;
    private final LedgerManager ledgerManager;

    // Expose Stats
    private final StatsLogger statsLogger;
    @StatsDoc(
        name = REPLICATE_EXCEPTION,
        help = "replication related exceptions"
    )
    private final StatsLogger exceptionLogger;
    @StatsDoc(
        name = REREPLICATE_OP,
        help = "operation stats of re-replicating ledgers"
    )
    private final OpStatsLogger rereplicateOpStats;
    @StatsDoc(
        name = NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED,
        help = "the number of ledgers re-replicated"
    )
    private final Counter numLedgersReplicated;
    @StatsDoc(
        name = NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER,
        help = "the number of defer-ledger-lock-releases of failed ledgers"
    )
    private final Counter numDeferLedgerLockReleaseOfFailedLedger;
    @StatsDoc(
            name = NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION,
            help = "the number of entries ReplicationWorker unable to read"
        )
    private final Counter numEntriesUnableToReadForReplication;
    @StatsDoc(
            name = NUM_NOT_ADHERING_PLACEMENT_LEDGERS_REPLICATED,
            help = "the number of not adhering placement policy ledgers re-replicated"
    )
    private final Counter numNotAdheringPlacementLedgersReplicated;
    private final Map<String, Counter> exceptionCounters;
    final LoadingCache<Long, AtomicInteger> replicationFailedLedgers;
    final LoadingCache<Long, ConcurrentSkipListSet<Long>> unableToReadEntriesForReplication;

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     *
     * @param conf
     *            - configurations
     */
    public ReplicationWorker(final ServerConfiguration conf)
            throws CompatibilityException, UnavailableException,
            InterruptedException, IOException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     *
     * @param conf
     *            - configurations
     * @param statsLogger
     *            - stats logger
     */
    public ReplicationWorker(final ServerConfiguration conf,
                             StatsLogger statsLogger)
            throws CompatibilityException, UnavailableException,
            InterruptedException, IOException {
        this(conf, Auditor.createBookKeeperClient(conf), true, statsLogger);
    }

    ReplicationWorker(final ServerConfiguration conf,
                      BookKeeper bkc,
                      boolean ownBkc,
                      StatsLogger statsLogger)
            throws CompatibilityException, InterruptedException, UnavailableException {
        this.conf = conf;
        this.bkc = bkc;
        this.ownBkc = ownBkc;

        this.underreplicationManager = bkc.getLedgerManagerFactory().newLedgerUnderreplicationManager();
        this.ledgerManager = bkc.getLedgerManagerFactory().newLedgerManager();
        this.admin = new BookKeeperAdmin(bkc, statsLogger, new ClientConfiguration(conf));
        this.ledgerChecker = new LedgerChecker(bkc);
        this.workerThread = new BookieThread(this, "ReplicationWorker");
        this.openLedgerRereplicationGracePeriod = conf
                .getOpenLedgerRereplicationGracePeriod();
        this.lockReleaseOfFailedLedgerGracePeriod = conf.getLockReleaseOfFailedLedgerGracePeriod();
        this.baseBackoffForLockReleaseOfFailedLedger = this.lockReleaseOfFailedLedgerGracePeriod
                / (long) (Math.pow(2, NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS));
        this.rwRereplicateBackoffMs = conf.getRwRereplicateBackoffMs();
        this.pendingReplicationTimer = new Timer("PendingReplicationTimer");
        this.replicationFailedLedgers = CacheBuilder.newBuilder().maximumSize(REPLICATED_FAILED_LEDGERS_MAXSIZE)
                .build(new CacheLoader<Long, AtomicInteger>() {
                    @Override
                    public AtomicInteger load(Long key) throws Exception {
                        return new AtomicInteger();
                    }
                });
        this.unableToReadEntriesForReplication = CacheBuilder.newBuilder()
                .maximumSize(REPLICATED_FAILED_LEDGERS_MAXSIZE)
                .build(new CacheLoader<Long, ConcurrentSkipListSet<Long>>() {
                    @Override
                    public ConcurrentSkipListSet<Long> load(Long key) throws Exception {
                        return new ConcurrentSkipListSet<Long>();
                    }
                });

        // Expose Stats
        this.statsLogger = statsLogger;
        this.exceptionLogger = statsLogger.scope(REPLICATE_EXCEPTION);
        this.rereplicateOpStats = this.statsLogger.getOpStatsLogger(REREPLICATE_OP);
        this.numLedgersReplicated = this.statsLogger.getCounter(NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED);
        this.numDeferLedgerLockReleaseOfFailedLedger = this.statsLogger
                .getCounter(NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER);
        this.numEntriesUnableToReadForReplication = this.statsLogger
                .getCounter(NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION);
        this.numNotAdheringPlacementLedgersReplicated = this.statsLogger
                .getCounter(NUM_NOT_ADHERING_PLACEMENT_LEDGERS_REPLICATED);
        this.exceptionCounters = new HashMap<String, Counter>();
        this.onReadEntryFailureCallback = (ledgerid, entryid) -> {
            numEntriesUnableToReadForReplication.inc();
            unableToReadEntriesForReplication.getUnchecked(ledgerid).add(entryid);
        };
    }

    /**
     * Start the replication worker.
     */
    public void start() {
        this.workerThread.start();
    }

    @Override
    public void run() {
        // 标记复制工作者线程已启动
        workerRunning = true;
        // 主循环，线程只要处于运行状态就持续尝试重新复制
        while (workerRunning) {
            try {
                // 重新复制账本片段。如果返回false，表示本轮复制失败，需要重试
                if (!rereplicate()) {
                    LOG.warn("复制账本片段时失败");
                    // 复制失败后，等待一段退避（backoff）时间再重试
                    waitBackOffTime(rwRereplicateBackoffMs);
                }
            } catch (InterruptedException e) {
                // 捕获线程中断异常，记录日志并优雅关闭线程，结束循环
                LOG.error("复制账本片段时被中断", e);
                shutdown();
                Thread.currentThread().interrupt();
                return;
            } catch (BKException e) {
                // 捕获 BookKeeper 相关异常，记录日志后等待退避时间，重试
                LOG.error("复制账本片段时出现 BKException", e);
                waitBackOffTime(rwRereplicateBackoffMs);
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                // 遇到不可恢复的复制异常，需直接关闭线程，结束循环
                LOG.error("遇到不可恢复的复制异常，无法继续复制账本片段", nre);
                shutdown();
                return;
            } catch (UnavailableException e) {
                // 捕获部分资源（如ZooKeeper、BookKeeper节点）不可用的异常，
                // 记录日志、等待退避时间，必要时优雅关闭线程
                LOG.error("复制账本片段时出现 UnavailableException", e);
                waitBackOffTime(rwRereplicateBackoffMs);
                // 检查当前线程是否被中断，若被中断则关闭并结束循环
                if (Thread.currentThread().isInterrupted()) {
                    LOG.error("复制账本片段过程中发生中断");
                    shutdown();
                    return;
                }
            }
        }
        // 复制工作者主循环退出日志
        LOG.info("ReplicationWorker 已退出主循环!");
    }


    private static void waitBackOffTime(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Replicates the under replicated fragments from failed bookie ledger to
     * targetBookie.
     */
    private boolean rereplicate() throws InterruptedException, BKException,
            UnavailableException {
        long ledgerIdToReplicate = underreplicationManager
                .getLedgerToRereplicate();

        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            success = rereplicate(ledgerIdToReplicate);
        } finally {
            long latencyMillis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            if (success) {
                rereplicateOpStats.registerSuccessfulEvent(latencyMillis, TimeUnit.MILLISECONDS);
            } else {
                rereplicateOpStats.registerFailedEvent(latencyMillis, TimeUnit.MILLISECONDS);
            }
        }
        return success;
    }

    private void logBKExceptionAndReleaseLedger(BKException e, long ledgerIdToReplicate)
        throws UnavailableException {
        LOG.info("{} while"
                + " rereplicating ledger {}."
                + " Enough Bookies might not have available"
                + " So, no harm to continue",
            e.getClass().getSimpleName(),
            ledgerIdToReplicate);
        underreplicationManager
            .releaseUnderreplicatedLedger(ledgerIdToReplicate);
        getExceptionCounter(e.getClass().getSimpleName()).inc();
    }

    private boolean tryReadingFaultyEntries(LedgerHandle lh, LedgerFragment ledgerFragment) {
        long ledgerId = lh.getId();
        ConcurrentSkipListSet<Long> entriesUnableToReadForThisLedger = unableToReadEntriesForReplication
                .getIfPresent(ledgerId);
        if (entriesUnableToReadForThisLedger == null) {
            return true;
        }
        long firstEntryIdOfFragment = ledgerFragment.getFirstEntryId();
        long lastEntryIdOfFragment = ledgerFragment.getLastKnownEntryId();
        NavigableSet<Long> entriesOfThisFragmentUnableToRead = entriesUnableToReadForThisLedger
                .subSet(firstEntryIdOfFragment, true, lastEntryIdOfFragment, true);
        if (entriesOfThisFragmentUnableToRead.isEmpty()) {
            return true;
        }
        final CountDownLatch multiReadComplete = new CountDownLatch(1);
        final AtomicInteger numOfResponsesToWaitFor = new AtomicInteger(entriesOfThisFragmentUnableToRead.size());
        final AtomicInteger returnRCValue = new AtomicInteger(BKException.Code.OK);
        for (long entryIdToRead : entriesOfThisFragmentUnableToRead) {
            if (multiReadComplete.getCount() == 0) {
                /*
                 * if an asyncRead request had already failed then break the
                 * loop.
                 */
                break;
            }
            lh.asyncReadEntries(entryIdToRead, entryIdToRead, (rc, ledHan, seq, ctx) -> {
                long thisEntryId = (Long) ctx;
                if (rc == BKException.Code.OK) {
                    while (seq.hasMoreElements()) {
                        LedgerEntry entry = seq.nextElement();
                        entry.getEntryBuffer().release();
                    }
                    entriesUnableToReadForThisLedger.remove(thisEntryId);
                    if (numOfResponsesToWaitFor.decrementAndGet() == 0) {
                        multiReadComplete.countDown();
                    }
                } else {
                    LOG.error("Received error: {} while trying to read entry: {} of ledger: {} in ReplicationWorker",
                            rc, entryIdToRead, ledgerId);
                    returnRCValue.compareAndSet(BKException.Code.OK, rc);
                    /*
                     * on receiving a failure error response, multiRead can be
                     * marked completed, since there is not need to wait for
                     * other responses.
                     */
                    multiReadComplete.countDown();
                }
            }, entryIdToRead);
        }
        try {
            multiReadComplete.await();
        } catch (InterruptedException e) {
            LOG.error("Got interrupted exception while trying to read entries", e);
            Thread.currentThread().interrupt();  // set interrupt flag
            return false;
        }
        return (returnRCValue.get() == BKException.Code.OK);
    }

    private Set<LedgerFragment> getNeedRepairedPlacementNotAdheringFragments(LedgerHandle lh) {
        if (!conf.isRepairedPlacementPolicyNotAdheringBookieEnable()) {
            return Collections.emptySet();
        }
        long ledgerId = lh.getId();
        Set<LedgerFragment> placementNotAdheringFragments = new HashSet<>();
        CompletableFuture<Versioned<LedgerMetadata>> future = ledgerManager.readLedgerMetadata(
                ledgerId).whenComplete((metadataVer, exception) -> {
            if (exception == null) {
                LedgerMetadata metadata = metadataVer.getValue();
                int writeQuorumSize = metadata.getWriteQuorumSize();
                int ackQuorumSize = metadata.getAckQuorumSize();
                if (!metadata.isClosed()) {
                    return;
                }
                Long curEntryId = null;
                EnsemblePlacementPolicy.PlacementPolicyAdherence previousSegmentAdheringToPlacementPolicy = null;

                for (Map.Entry<Long, ? extends List<BookieId>> entry : metadata.getAllEnsembles().entrySet()) {
                    if (curEntryId != null) {
                        if (EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL
                                == previousSegmentAdheringToPlacementPolicy) {
                            LedgerFragment ledgerFragment = new LedgerFragment(lh, curEntryId,
                                    entry.getKey() - 1, new HashSet<>());
                            ledgerFragment.setReplicateType(LedgerFragment.ReplicateType.DATA_NOT_ADHERING_PLACEMENT);
                            placementNotAdheringFragments.add(ledgerFragment);
                        }
                    }
                    previousSegmentAdheringToPlacementPolicy =
                            admin.isEnsembleAdheringToPlacementPolicy(entry.getValue(),
                                    writeQuorumSize, ackQuorumSize);
                    curEntryId = entry.getKey();
                }
                if (curEntryId != null) {
                    if (EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL
                            == previousSegmentAdheringToPlacementPolicy) {
                        long lastEntry = lh.getLedgerMetadata().getLastEntryId();
                        LedgerFragment ledgerFragment = new LedgerFragment(lh, curEntryId, lastEntry,
                                new HashSet<>());
                        ledgerFragment.setReplicateType(LedgerFragment.ReplicateType.DATA_NOT_ADHERING_PLACEMENT);
                        placementNotAdheringFragments.add(ledgerFragment);
                    }
                }
            } else if (BKException.getExceptionCode(exception)
                    == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring replication of already deleted ledger {}", ledgerId);
                }
            } else {
                LOG.warn("Unable to read the ledger: {} information", ledgerId);
            }
        });
        try {
            FutureUtils.result(future);
        } catch (Exception e) {
            LOG.warn("Check ledger need repaired placement not adhering bookie failed", e);
            return Collections.emptySet();
        }
        return placementNotAdheringFragments;
    }

    /**
     * 对指定 ledgerId 的账本进行碎片（fragment）重新复制（rereplicate）。
     * 包括读取未充分复制片段、数据恢复、及锁的管理。
     *
     * @param ledgerIdToReplicate 需修复的账本ID
     * @return true: 账本已完全修复；false: 仍有待修复或 defer（未释放锁）
     * @throws InterruptedException   线程中断异常
     * @throws BKException           BookKeeper 相关异常
     * @throws UnavailableException  某些资源不可用异常（如ZooKeeper等）
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean rereplicate(long ledgerIdToReplicate) throws InterruptedException, BKException,
            UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("准备复制账本片段，LedgerID: {}", ledgerIdToReplicate);
        }

        boolean deferLedgerLockRelease = false; // 是否延迟释放ledger锁

        // 使用 LedgerAdmin 打开 ledger（无恢复模式），并自动管理句柄生命周期
        try (LedgerHandle lh = admin.openLedgerNoRecovery(ledgerIdToReplicate)) {
            // 获取所有未充分复制的 LedgerFragment（即 underreplicated fragments）
            Set<LedgerFragment> fragments = getUnderreplicatedFragments(
                    lh, conf.getAuditorLedgerVerificationPercentage());

            if (LOG.isDebugEnabled()) {
                LOG.debug("发现 {} 个待修复片段，LedgerID: {}", fragments, ledgerIdToReplicate);
            }

            boolean foundOpenFragments = false;  // 标记是否存在未关闭的片段
            long numFragsReplicated = 0;         // 本次成功修复的片段计数
            long numNotAdheringPlacementFragsReplicated = 0; // 非正常副本布局的计数

            // 遍历所有未充分复制片段
            for (LedgerFragment ledgerFragment : fragments) {
                if (!ledgerFragment.isClosed()) {
                    // 片段未关闭（有客户端在写），跳过，稍后重试
                    foundOpenFragments = true;
                    continue;
                }
                // 首先尝试读取故障条目，校验数据可恢复性
                if (!tryReadingFaultyEntries(lh, ledgerFragment)) {
                    LOG.error("读取故障条目失败，放弃修复 ledgerFragment {}", ledgerFragment);
                    continue;
                }
                try {
                    // 对片段执行实际修复/复制操作
                    admin.replicateLedgerFragment(lh, ledgerFragment, onReadEntryFailureCallback);
                    numFragsReplicated++;
                    // 统计因副本分布不符合要求而修复的片段
                    if (ledgerFragment.getReplicateType() == LedgerFragment.ReplicateType.DATA_NOT_ADHERING_PLACEMENT) {
                        numNotAdheringPlacementFragsReplicated++;
                    }
                } catch (BKException.BKBookieHandleNotAvailableException e) {
                    LOG.warn("修复片段时 bookie 不可用异常", e);
                } catch (BKException.BKLedgerRecoveryException e) {
                    LOG.warn("账本恢复异常", e);
                } catch (BKException.BKNotEnoughBookiesException e) {
                    LOG.warn("可用 bookie 不足，无法修复片段", e);
                }
            }

            // 统计本批次修复情况
            if (numFragsReplicated > 0) {
                numLedgersReplicated.inc();
            }
            if (numNotAdheringPlacementFragsReplicated > 0) {
                numNotAdheringPlacementLedgersReplicated.inc();
            }

            // 处理未关闭的片段或最后一段（open segment）缺失 bookie 的场景
            if (foundOpenFragments || isLastSegmentOpenAndMissingBookies(lh)) {
                deferLedgerLockRelease = true; // 设置延迟释放锁
                deferLedgerLockRelease(ledgerIdToReplicate); // 做延迟释放
                return false; // 本轮不成功，等待稍后由 worker 继续抢占处理
            }

            // 再次校验是否还有未修复片段
            fragments = getUnderreplicatedFragments(lh, conf.getAuditorLedgerVerificationPercentage());
            if (fragments.size() == 0) {
                // 已全部修复，记录日志并做标记
                LOG.info("Ledger 修复成功，ledger id: " + ledgerIdToReplicate);
                underreplicationManager.markLedgerReplicated(ledgerIdToReplicate);
                return true;
            } else {
                // 存在未修复片段（可能本次遇到新异常），延迟释放锁，下次继续竞争处理
                deferLedgerLockRelease = true;
                deferLedgerLockReleaseOfFailedLedger(ledgerIdToReplicate);
                numDeferLedgerLockReleaseOfFailedLedger.inc();
                return false;
            }

        } catch (BKNoSuchLedgerExistsOnMetadataServerException e) {
            // 账本已被删除（如用户操作），无需修复，直接标记并返回
            LOG.info("账本已被删除，无需修复，ledger id: " + ledgerIdToReplicate);
            underreplicationManager.markLedgerReplicated(ledgerIdToReplicate);
            getExceptionCounter("BKNoSuchLedgerExistsOnMetadataServerException").inc();
            return false;
        } catch (BKNotEnoughBookiesException e) {
            // 可用 Bookie 不足，需要特殊处理，并上抛异常
            logBKExceptionAndReleaseLedger(e, ledgerIdToReplicate);
            throw e;
        } catch (BKException e) {
            // 其它 BookKeeper 异常，释放锁，记录日志
            logBKExceptionAndReleaseLedger(e, ledgerIdToReplicate);
            return false;
        } finally {
            // 最终（即使异常）都保证 ledger 的处理锁会被释放（除非 defer 状态已明确延迟释放）
            if (!deferLedgerLockRelease) {
                try {
                    underreplicationManager.releaseUnderreplicatedLedger(ledgerIdToReplicate);
                } catch (UnavailableException e) {
                    LOG.error("释放 ledger 修复锁时出现 UnavailableException：" + ledgerIdToReplicate, e);
                    shutdown();
                }
            }
        }
    }



    /**
     * When checking the fragments of a ledger, there is a corner case
     * where if the last segment/ensemble is open, but nothing has been written to
     * some of the quorums in the ensemble, bookies can fail without any action being
     * taken. This is fine, until enough bookies fail to cause a quorum to become
     * unavailable, by which time the ledger is unrecoverable.
     *
     * <p>For example, if in a E3Q2, only 1 entry is written and the last bookie
     * in the ensemble fails, nothing has been written to it, so nothing needs to be
     * recovered. But if the second to last bookie fails, we've now lost quorum for
     * the second entry, so it's impossible to see if the second has been written or
     * not.
     *
     * <p>To avoid this situation, we need to check if bookies in the final open ensemble
     * are unavailable, and take action if so. The action to take is to close the ledger,
     * after a grace period as the writing client may replace the faulty bookie on its
     * own.
     *
     * <p>Missing bookies in closed ledgers are fine, as we know the last confirmed add, so
     * we can tell which entries are supposed to exist and rereplicate them if necessary.
     *
     * <p>Another corner case is that there are multiple ensembles in the ledger and the last
     * segment/ensemble is open, but nothing has been written to some quorums in the ensemble.
     * For the v2 protocol, this ledger's lastAddConfirm entry is the last segment/ensemble's `key - 2`,
     * not `key - 2`, the explanation please refer to: https://github.com/apache/bookkeeper/pull/3917.
     * If we treat the penultimate segment/ensemble as closed state, we will can't replicate
     * the last entry in the segment. So in this case, we should also check if the penultimate
     * segment/ensemble has missing bookies.
     */
    private boolean isLastSegmentOpenAndMissingBookies(LedgerHandle lh) throws BKException {
        LedgerMetadata md = admin.getLedgerMetadata(lh);
        if (md.isClosed()) {
            return false;
        }

        SortedMap<Long, ? extends List<BookieId>> ensembles = admin.getLedgerMetadata(lh).getAllEnsembles();
        List<BookieId> finalEnsemble = ensembles.get(ensembles.lastKey());
        if (ensembles.size() > 1 && lh.getLastAddConfirmed() < ensembles.lastKey() - 1) {
            finalEnsemble = new ArrayList<>(finalEnsemble);
            finalEnsemble.addAll((new TreeMap<>(ensembles)).floorEntry(ensembles.lastKey() - 1).getValue());
        }
        Collection<BookieId> available = admin.getAvailableBookies();
        for (BookieId b : finalEnsemble) {
            if (!available.contains(b)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Bookie {} is missing from the list of Available Bookies. ledger {}:ensemble {}.",
                            b, lh.getId(), finalEnsemble);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * 获取指定账本（LedgerHandle）下所有未充分复制的片段（fragment）。
     * 优先处理数据丢失片段（data_loss），其后处理副本分布不符合要求的片段（not_adhering_placement）。
     * 如果同时属于两者，本轮只修复数据丢失，待其修复后若仍不符合分布要求，将再次被 Auditor 标记处理。
     *
     * @param lh                        账本句柄
     * @param ledgerVerificationPercentage 账本校验百分比（可用于采样，提升性能）
     * @return 需要修复的所有未充分复制片段（LedgerFragment 集合）
     * @throws InterruptedException     线程中断异常
     */
    Set<LedgerFragment> getUnderreplicatedFragments(LedgerHandle lh, Long ledgerVerificationPercentage)
            throws InterruptedException {
        // 首选要修复的数据丢失片段。如果一个片段既 data_loss 又 not_adhering_placement，优先本轮只修数据丢失
        Set<LedgerFragment> underreplicatedFragments = new HashSet<>();

        // 找到所有数据丢失片段（损坏片段，需优先修复），可指定校验比例
        Set<LedgerFragment> dataLossFragments = getDataLossFragments(lh, ledgerVerificationPercentage);
        underreplicatedFragments.addAll(dataLossFragments);

        // 找到所有副本分布不符要求的片段（比如副本没按要求落在不同 bookie 上）
        Set<LedgerFragment> notAdheringFragments = getNeedRepairedPlacementNotAdheringFragments(lh);

        // 如果某个片段既在 data_loss 又在 not_adhering_placement，跳过，只修一次（优先 data_loss）
        // 其余不重复的片段正常加入本轮修复集合
        for (LedgerFragment notAdheringFragment : notAdheringFragments) {
            if (!checkFragmentRepeat(underreplicatedFragments, notAdheringFragment)) {
                underreplicatedFragments.add(notAdheringFragment);
            }
        }
        // 返回需要修复的所有片段集合
        return underreplicatedFragments;
    }


    private Set<LedgerFragment> getDataLossFragments(LedgerHandle lh, Long ledgerVerificationPercentage)
            throws InterruptedException {
        CheckerCallback checkerCb = new CheckerCallback();
        ledgerChecker.checkLedger(lh, checkerCb, ledgerVerificationPercentage);
        return checkerCb.waitAndGetResult();
    }

    private boolean checkFragmentRepeat(Set<LedgerFragment> fragments, LedgerFragment needChecked) {
        for (LedgerFragment fragment : fragments) {
            if (fragment.getLedgerId() == needChecked.getLedgerId()
                    && fragment.getFirstEntryId() == needChecked.getFirstEntryId()
                    && fragment.getLastKnownEntryId() == needChecked.getLastKnownEntryId()) {
                return true;
            }
        }
        return false;
    }

    void scheduleTaskWithDelay(TimerTask timerTask, long delayPeriod) {
        pendingReplicationTimer.schedule(timerTask, delayPeriod);
    }

    /**
     * Schedules a timer task for releasing the lock which will be scheduled
     * after open ledger fragment replication time. Ledger will be fenced if it
     * is still in open state when timer task fired.
     */
    private void deferLedgerLockRelease(final long ledgerId) {
        long gracePeriod = this.openLedgerRereplicationGracePeriod;
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                boolean isRecoveryOpen = false;
                LedgerHandle lh = null;
                try {
                    lh = admin.openLedgerNoRecovery(ledgerId);
                    if (isLastSegmentOpenAndMissingBookies(lh)) {
                        // Need recovery open, close the old ledger handle.
                        lh.close();
                        // Recovery open could result in client write failure.
                        LOG.warn("Missing bookie(s) from last segment. Opening Ledger {} for Recovery.", ledgerId);
                        lh = admin.openLedger(ledgerId);
                        isRecoveryOpen = true;
                    }
                    if (!isRecoveryOpen){
                        Set<LedgerFragment> fragments =
                            getUnderreplicatedFragments(lh, conf.getAuditorLedgerVerificationPercentage());
                        for (LedgerFragment fragment : fragments) {
                            if (!fragment.isClosed()) {
                                // Need recovery open, close the old ledger handle.
                                lh.close();
                                // Recovery open could result in client write failure.
                                LOG.warn("Open Fragment{}. Opening Ledger {} for Recovery.",
                                        fragment.getEnsemble(), ledgerId);
                                lh = admin.openLedger(ledgerId);
                                isRecoveryOpen = true;
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("InterruptedException while fencing the ledger {}"
                            + " for rereplication of postponed ledgers", ledgerId, e);
                } catch (BKNoSuchLedgerExistsOnMetadataServerException bknsle) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ledger {} was deleted, safe to continue", ledgerId, bknsle);
                    }
                } catch (BKException e) {
                    LOG.error("BKException while fencing the ledger {}"
                            + " for rereplication of postponed ledgers", ledgerId, e);
                } finally {
                    try {
                        if (lh != null) {
                            lh.close();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("InterruptedException while closing ledger {}", ledgerId, e);
                    } catch (BKException e) {
                        // Lets go ahead and release the lock. Catch actual
                        // exception in normal replication flow and take
                        // action.
                        LOG.warn("BKException while closing ledger {} ", ledgerId, e);
                    } finally {
                        try {
                            underreplicationManager
                                    .releaseUnderreplicatedLedger(ledgerId);
                        } catch (UnavailableException e) {
                            LOG.error("UnavailableException while replicating fragments of ledger {}",
                                    ledgerId, e);
                            shutdown();
                        }
                    }
                }
            }
        };
        scheduleTaskWithDelay(timerTask, gracePeriod);
    }

    /**
     * Schedules a timer task for releasing the lock.
     */
    private void deferLedgerLockReleaseOfFailedLedger(final long ledgerId) {
        int numOfTimesFailedSoFar = replicationFailedLedgers.getUnchecked(ledgerId).getAndIncrement();
        /*
         * for the first NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS retrials do
         * exponential backoff, starting from
         * baseBackoffForLockReleaseOfFailedLedger
         */
        long delayOfLedgerLockReleaseInMSecs = (numOfTimesFailedSoFar >= NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS)
                ? this.lockReleaseOfFailedLedgerGracePeriod
                : this.baseBackoffForLockReleaseOfFailedLedger * (int) Math.pow(2, numOfTimesFailedSoFar);
        LOG.error(
                "ReplicationWorker failed to replicate Ledger : {} for {} number of times, "
                + "so deferring the ledger lock release by {} msecs",
                ledgerId, numOfTimesFailedSoFar, delayOfLedgerLockReleaseInMSecs);
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    underreplicationManager.releaseUnderreplicatedLedger(ledgerId);
                } catch (UnavailableException e) {
                    LOG.error("UnavailableException while replicating fragments of ledger {}", ledgerId, e);
                    shutdown();
                }
            }
        };
        scheduleTaskWithDelay(timerTask, delayOfLedgerLockReleaseInMSecs);
    }

    /**
     * Stop the replication worker service.
     */
    public void shutdown() {
        LOG.info("Shutting down replication worker");

        synchronized (this) {
            if (!workerRunning) {
                return;
            }
            workerRunning = false;
        }
        LOG.info("Shutting down ReplicationWorker");
        this.pendingReplicationTimer.cancel();
        try {
            this.workerThread.interrupt();
            this.workerThread.join();
        } catch (InterruptedException e) {
            LOG.error("Interrupted during shutting down replication worker : ",
                    e);
            Thread.currentThread().interrupt();
        }
        if (ownBkc) {
            try {
                bkc.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing the Bookie client", e);
                Thread.currentThread().interrupt();
            } catch (BKException e) {
                LOG.warn("Exception while closing the Bookie client", e);
            }
        }
        try {
            underreplicationManager.close();
        } catch (UnavailableException e) {
            LOG.warn("Exception while closing the "
                    + "ZkLedgerUnderrepliationManager", e);
        }
    }

    /**
     * Gives the running status of ReplicationWorker.
     */
    @VisibleForTesting
    public boolean isRunning() {
        return workerRunning && workerThread.isAlive();
    }

    /**
     * Ledger checker call back.
     */
    private static class CheckerCallback implements
            GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }

        /**
         * Wait until operation complete call back comes and return the ledger
         * fragments set.
         */
        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }
    }

    private Counter getExceptionCounter(String name) {
        Counter counter = this.exceptionCounters.get(name);
        if (counter == null) {
            counter = this.exceptionLogger.getCounter(name);
            this.exceptionCounters.put(name, counter);
        }
        return counter;
    }

}
