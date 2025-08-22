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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.LedgerHandle.INVALID_ENTRY_ID;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BYTES_READ;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BYTES_WRITTEN;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_READ;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_WRITTEN;
import static org.apache.bookkeeper.replication.ReplicationStats.READ_DATA_LATENCY;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.WRITE_DATA_LATENCY;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the helper class for replicating the fragments from one bookie to
 * another.
 */
@StatsDoc(
    name = REPLICATION_WORKER_SCOPE,
    help = "Ledger fragment replicator related stats"
)
public class LedgerFragmentReplicator {

    // BookKeeper instance
    private BookKeeper bkc;
    private StatsLogger statsLogger;
    @StatsDoc(
        name = NUM_ENTRIES_READ,
        help = "Number of entries read by the replicator"
    )
    private final Counter numEntriesRead;
    @StatsDoc(
        name = NUM_BYTES_READ,
        help = "The distribution of size of entries read by the replicator"
    )
    private final OpStatsLogger numBytesRead;
    @StatsDoc(
        name = NUM_ENTRIES_WRITTEN,
        help = "Number of entries written by the replicator"
    )
    private final Counter numEntriesWritten;
    @StatsDoc(
        name = NUM_BYTES_WRITTEN,
        help = "The distribution of size of entries written by the replicator"
    )
    private final OpStatsLogger numBytesWritten;
    @StatsDoc(
            name = READ_DATA_LATENCY,
            help = "The distribution of latency of read entries by the replicator"
    )
    private final OpStatsLogger readDataLatency;
    @StatsDoc(
            name = WRITE_DATA_LATENCY,
            help = "The distribution of latency of write entries by the replicator"
    )
    private final OpStatsLogger writeDataLatency;

    protected Throttler replicationThrottle = null;

    private AtomicInteger averageEntrySize;

    private static final int INITIAL_AVERAGE_ENTRY_SIZE = 1024;
    private static final double AVERAGE_ENTRY_SIZE_RATIO = 0.8;
    private ClientConfiguration conf;

    public LedgerFragmentReplicator(BookKeeper bkc, StatsLogger statsLogger, ClientConfiguration conf) {
        this.bkc = bkc;
        this.statsLogger = statsLogger;
        numEntriesRead = this.statsLogger.getCounter(NUM_ENTRIES_READ);
        numBytesRead = this.statsLogger.getOpStatsLogger(NUM_BYTES_READ);
        numEntriesWritten = this.statsLogger.getCounter(NUM_ENTRIES_WRITTEN);
        numBytesWritten = this.statsLogger.getOpStatsLogger(NUM_BYTES_WRITTEN);
        readDataLatency = this.statsLogger.getOpStatsLogger(READ_DATA_LATENCY);
        writeDataLatency = this.statsLogger.getOpStatsLogger(WRITE_DATA_LATENCY);
        if (conf.getReplicationRateByBytes() > 0) {
            this.replicationThrottle = new Throttler(conf.getReplicationRateByBytes());
        }
        averageEntrySize = new AtomicInteger(INITIAL_AVERAGE_ENTRY_SIZE);
        this.conf = conf;
    }

    public LedgerFragmentReplicator(BookKeeper bkc, ClientConfiguration conf) {
        this(bkc, NullStatsLogger.INSTANCE, conf);
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(LedgerFragmentReplicator.class);

    private void replicateFragmentInternal(final LedgerHandle lh,
            final LedgerFragment lf,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final Set<BookieId> newBookies,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) throws InterruptedException {
        if (!lf.isClosed()) {
            LOG.error("Trying to replicate an unclosed fragment;"
                      + " This is not safe {}", lf);
            ledgerFragmentMcb.processResult(BKException.Code.UnclosedFragmentException,
                                            null, null);
            return;
        }
        Long startEntryId = lf.getFirstStoredEntryId();
        Long endEntryId = lf.getLastStoredEntryId();

        /*
         * if startEntryId is INVALID_ENTRY_ID then endEntryId should be
         * INVALID_ENTRY_ID and viceversa.
         */
        if (startEntryId == INVALID_ENTRY_ID ^ endEntryId == INVALID_ENTRY_ID) {
            LOG.error("For LedgerFragment: {}, seeing inconsistent firstStoredEntryId: {} and lastStoredEntryId: {}",
                    lf, startEntryId, endEntryId);
            assert false;
        }

        if (startEntryId > endEntryId || endEntryId <= INVALID_ENTRY_ID) {
            // for open ledger which there is no entry, the start entry id is 0,
            // the end entry id is -1.
            // we can return immediately to trigger forward read
            ledgerFragmentMcb.processResult(BKException.Code.OK, null, null);
            return;
        }

        /*
         * Now asynchronously replicate all of the entries for the ledger
         * fragment that were on the dead bookie.
         */
        int entriesToReplicateCnt = (int) (endEntryId - startEntryId + 1);
        MultiCallback ledgerFragmentEntryMcb = new MultiCallback(
                entriesToReplicateCnt, ledgerFragmentMcb, null, BKException.Code.OK,
                BKException.Code.LedgerRecoveryException);
        if (this.replicationThrottle != null) {
            this.replicationThrottle.resetRate(this.conf.getReplicationRateByBytes());
        }

        if (conf.isRecoveryBatchReadEnabled()
                && conf.getUseV2WireProtocol()
                && conf.isBatchReadEnabled()
                && lh.getLedgerMetadata().getEnsembleSize() == lh.getLedgerMetadata().getWriteQuorumSize()) {
            batchRecoverLedgerFragmentEntry(startEntryId, endEntryId, lh, ledgerFragmentEntryMcb,
                    newBookies, onReadEntryFailureCallback);

        } else {
            /*
             * Add all the entries to entriesToReplicate list from
             * firstStoredEntryId to lastStoredEntryID.
             */
            List<Long> entriesToReplicate = new LinkedList<Long>();
            long lastStoredEntryId = lf.getLastStoredEntryId();
            for (long i = lf.getFirstStoredEntryId(); i <= lastStoredEntryId; i++) {
                entriesToReplicate.add(i);
            }
            for (final Long entryId : entriesToReplicate) {
                recoverLedgerFragmentEntry(entryId, lh, ledgerFragmentEntryMcb,
                        newBookies, onReadEntryFailureCallback);
            }
        }

    }

    /**
     * 该方法用于复制 ledger 片段（LedgerFragment），该片段是存储在包含故障 Bookie 的副本组（ensemble）中的一段连续数据。
     * 会将该片段按照 rereplicationEntryBatchSize（批处理条目数）配置拆分为多个子片段，
     * 然后依次逐个重复制这些批次片段。所有批次片段重复制完成后，会用新的 Bookie 更新副本组（ensemble）信息。
     *
     * @param lh
     *            LedgerHandle，账本句柄
     * @param lf
     *            LedgerFragment，需要复制的账本片段
     * @param ledgerFragmentMcb
     *            AsyncCallback.VoidCallback，全部片段复制完成后的回调
     * @param targetBookieAddresses
     *            Set<BookieId>，用于替换的目标 Bookie 节点集合
     * @param onReadEntryFailureCallback
     *            读取 entry 失败时的回调
     */
    void replicate(
            final LedgerHandle lh,                     // 账本句柄
            final LedgerFragment lf,                   // 需要复制的账本片段
            final AsyncCallback.VoidCallback ledgerFragmentMcb, // 全部片段复制完成回调
            final Set<BookieId> targetBookieAddresses,          // 替换目标 Bookie 集合
            final BiConsumer<Long, Long> onReadEntryFailureCallback // 读取 entry 失败的回调
    ) throws InterruptedException {
        // 按配置批大小把片段拆分为多个子片段
        Set<LedgerFragment> partitionedFragments = splitIntoSubFragments(
                lh,
                lf,
                bkc.getConf().getRereplicationEntryBatchSize() // 批处理重复制条目数配置
        );
        LOG.info("Replicating fragment {} in {} sub fragments.",
                lf, partitionedFragments.size()); // 日志：当前片段拆分为多少子片段

        // 按批次逐个复制子片段（递归方式实现）
        replicateNextBatch(
                lh,
                partitionedFragments.iterator(),      // 所有子片段的 iterator
                ledgerFragmentMcb,                    // 最终回调
                targetBookieAddresses,                // 目标 Bookie
                onReadEntryFailureCallback            // 读取失败回调
        );
    }

    /**
     * 依次复制批处理 entry 子片段（Batched entry fragments）。
     *
     * @param lh
     *            LedgerHandle，账本句柄
     * @param fragments
     *            Iterator<LedgerFragment>，待复制的子片段迭代器
     * @param ledgerFragmentMcb
     *            全部完成时的回调
     * @param targetBookieAddresses
     *            目标 Bookie 集合
     * @param onReadEntryFailureCallback
     *            读取 entry 失败的回调
     */
    private void replicateNextBatch(
            final LedgerHandle lh,                        // 账本句柄
            final Iterator<LedgerFragment> fragments,     // 子片段迭代器
            final AsyncCallback.VoidCallback ledgerFragmentMcb, // 片段复制总回调
            final Set<BookieId> targetBookieAddresses,    // 目标 bookie 集合
            final BiConsumer<Long, Long> onReadEntryFailureCallback // 读取 entry 失败回调
    ) {
        if (fragments.hasNext()) {
            try {
                // 递归方式复制当前子片段
                replicateFragmentInternal(
                        lh,
                        fragments.next(),                     // 当前子片段
                        new AsyncCallback.VoidCallback() {    // 当前片段复制完成回调
                            @Override
                            public void processResult(int rc, String v, Object ctx) {
                                // 复制失败，直接调用总回调（结束流程），否则递归调用复制下一个子片段
                                if (rc != BKException.Code.OK) {
                                    ledgerFragmentMcb.processResult(rc, null, null);
                                } else {
                                    replicateNextBatch(lh, fragments,
                                            ledgerFragmentMcb,
                                            targetBookieAddresses,
                                            onReadEntryFailureCallback);
                                }
                            }
                        },
                        targetBookieAddresses,           // 目标 Bookie
                        onReadEntryFailureCallback       // 读取 entry 失败回调
                );
            } catch (InterruptedException e) {
                // 线程中断，通知回调异常
                ledgerFragmentMcb.processResult(
                        BKException.Code.InterruptedException, null, null);
                Thread.currentThread().interrupt();  // 设置中断标志
            }
        } else {
            // 所有子片段都已经复制完，回调成功
            ledgerFragmentMcb.processResult(BKException.Code.OK, null, null);
        }
    }


    /**
     * Split the full fragment into batched entry fragments by keeping
     * rereplicationEntryBatchSize of entries in each one and can treat them as
     * sub fragments.
     */
    static Set<LedgerFragment> splitIntoSubFragments(LedgerHandle lh,
            LedgerFragment ledgerFragment, long rereplicationEntryBatchSize) {
        Set<LedgerFragment> fragments = new HashSet<LedgerFragment>();
        if (rereplicationEntryBatchSize <= 0) {
            // rereplicationEntryBatchSize can not be 0 or less than 0,
            // returning with the current fragment
            fragments.add(ledgerFragment);
            return fragments;
        }

        long firstEntryId = ledgerFragment.getFirstStoredEntryId();
        long lastEntryId = ledgerFragment.getLastStoredEntryId();

        /*
         * if firstEntryId is INVALID_ENTRY_ID then lastEntryId should be
         * INVALID_ENTRY_ID and viceversa.
         */
        if (firstEntryId == INVALID_ENTRY_ID ^ lastEntryId == INVALID_ENTRY_ID) {
            LOG.error("For LedgerFragment: {}, seeing inconsistent firstStoredEntryId: {} and lastStoredEntryId: {}",
                    ledgerFragment, firstEntryId, lastEntryId);
            assert false;
        }

        long numberOfEntriesToReplicate = firstEntryId == INVALID_ENTRY_ID ? 0 : (lastEntryId - firstEntryId) + 1;
        long splitsWithFullEntries = numberOfEntriesToReplicate
                / rereplicationEntryBatchSize;

        if (splitsWithFullEntries == 0) {// only one fragment
            fragments.add(ledgerFragment);
            return fragments;
        }

        long fragmentSplitLastEntry = 0;
        for (int i = 0; i < splitsWithFullEntries; i++) {
            fragmentSplitLastEntry = (firstEntryId + rereplicationEntryBatchSize) - 1;
            fragments.add(new LedgerFragment(lh, firstEntryId,
                    fragmentSplitLastEntry, ledgerFragment.getBookiesIndexes()));
            firstEntryId = fragmentSplitLastEntry + 1;
        }

        long lastSplitWithPartialEntries = numberOfEntriesToReplicate
                % rereplicationEntryBatchSize;
        if (lastSplitWithPartialEntries > 0) {
            fragments.add(new LedgerFragment(lh, firstEntryId, firstEntryId
                    + lastSplitWithPartialEntries - 1, ledgerFragment
                    .getBookiesIndexes()));
        }
        return fragments;
    }

    /**
     * This method asynchronously recovers a specific ledger entry by reading
     * the values via the BookKeeper Client (which would read it from the other
     * replicas) and then writing it to the chosen new bookie.
     *
     * @param entryId
     *            Ledger Entry ID to recover.
     * @param lh
     *            LedgerHandle for the ledger
     * @param ledgerFragmentEntryMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger entry.
     * @param newBookies
     *            New bookies we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    void recoverLedgerFragmentEntry(final Long entryId,
            final LedgerHandle lh,
            final AsyncCallback.VoidCallback ledgerFragmentEntryMcb,
            final Set<BookieId> newBookies,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) throws InterruptedException {
        final long ledgerId = lh.getId();
        final AtomicInteger numCompleted = new AtomicInteger(0);
        final AtomicBoolean completed = new AtomicBoolean(false);

        if (replicationThrottle != null) {
            replicationThrottle.acquire(averageEntrySize.get());
        }

        final WriteCallback multiWriteCallback = new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                if (rc != BKException.Code.OK) {
                    LOG.error("BK error writing entry for ledgerId: {}, entryId: {}, bookie: {}",
                            ledgerId, entryId, addr, BKException.create(rc));
                    if (completed.compareAndSet(false, true)) {
                        ledgerFragmentEntryMcb.processResult(rc, null, null);
                    }
                } else {
                    numEntriesWritten.inc();
                    if (ctx instanceof Long) {
                        numBytesWritten.registerSuccessfulValue((Long) ctx);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Success writing ledger id {}, entry id {} to a new bookie {}!",
                                ledgerId, entryId, addr);
                    }
                    if (numCompleted.incrementAndGet() == newBookies.size() && completed.compareAndSet(false, true)) {
                        ledgerFragmentEntryMcb.processResult(rc, null, null);
                    }
                }
            }
        };

        long startReadEntryTime = MathUtils.nowInNano();
        /*
         * Read the ledger entry using the LedgerHandle. This will allow us to
         * read the entry from one of the other replicated bookies other than
         * the dead one.
         */
        lh.asyncReadEntries(entryId, entryId, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh,
                    Enumeration<LedgerEntry> seq, Object ctx) {
                if (rc != BKException.Code.OK) {
                    LOG.error("BK error reading ledger entry: " + entryId,
                            BKException.create(rc));
                    onReadEntryFailureCallback.accept(ledgerId, entryId);
                    ledgerFragmentEntryMcb.processResult(rc, null, null);
                    return;
                }

                readDataLatency.registerSuccessfulEvent(MathUtils.elapsedNanos(startReadEntryTime),
                        TimeUnit.NANOSECONDS);

                /*
                 * Now that we've read the ledger entry, write it to the new
                 * bookie we've selected.
                 */
                LedgerEntry entry = seq.nextElement();
                byte[] data = entry.getEntry();
                final long dataLength = data.length;
                numEntriesRead.inc();
                numBytesRead.registerSuccessfulValue(dataLength);

                ReferenceCounted toSend = lh.getDigestManager()
                        .computeDigestAndPackageForSending(entryId,
                                lh.getLastAddConfirmed(), entry.getLength(),
                                Unpooled.wrappedBuffer(data, 0, data.length),
                                lh.getLedgerKey(),
                                BookieProtocol.FLAG_RECOVERY_ADD
                                );
                if (replicationThrottle != null) {
                    if (toSend instanceof ByteBuf) {
                        updateAverageEntrySize(((ByteBuf) toSend).readableBytes());
                    } else if (toSend instanceof ByteBufList) {
                        updateAverageEntrySize(((ByteBufList) toSend).readableBytes());
                    }
                }
                for (BookieId newBookie : newBookies) {
                    long startWriteEntryTime = MathUtils.nowInNano();
                    bkc.getBookieClient().addEntry(newBookie, lh.getId(),
                            lh.getLedgerKey(), entryId, toSend,
                            multiWriteCallback, dataLength, BookieProtocol.FLAG_RECOVERY_ADD,
                            false, WriteFlag.NONE);
                    writeDataLatency.registerSuccessfulEvent(
                           MathUtils.elapsedNanos(startWriteEntryTime), TimeUnit.NANOSECONDS);
                }
                toSend.release();
            }
        }, null);
    }

    void batchRecoverLedgerFragmentEntry(final long startEntryId,
                                         final long endEntryId,
                                         final LedgerHandle lh,
                                         final AsyncCallback.VoidCallback ledgerFragmentMcb,
                                         final Set<BookieId> newBookies,
                                         final BiConsumer<Long, Long> onReadEntryFailureCallback)
            throws InterruptedException {
        int entriesToReplicateCnt = (int) (endEntryId - startEntryId + 1);
        int maxBytesToReplicate = conf.getReplicationRateByBytes();
        if (replicationThrottle != null) {
            if (maxBytesToReplicate != -1 && maxBytesToReplicate > averageEntrySize.get() * entriesToReplicateCnt) {
                maxBytesToReplicate = averageEntrySize.get() * entriesToReplicateCnt;
            }
            replicationThrottle.acquire(maxBytesToReplicate);
        }

        lh.asyncBatchReadEntries(startEntryId, entriesToReplicateCnt, maxBytesToReplicate,
            new ReadCallback() {
                @Override
                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                    if (rc != BKException.Code.OK) {
                        LOG.error("BK error reading ledger entries: {} - {}",
                                startEntryId, endEntryId, BKException.create(rc));
                        onReadEntryFailureCallback.accept(lh.getId(), startEntryId);
                        for (int i = 0; i < entriesToReplicateCnt; i++) {
                            ledgerFragmentMcb.processResult(rc, null, null);
                        }
                        return;
                    }
                    long lastEntryId = startEntryId;
                    while (seq.hasMoreElements()) {
                        LedgerEntry entry = seq.nextElement();
                        lastEntryId = entry.getEntryId();
                        byte[] data = entry.getEntry();
                        final long dataLength = data.length;
                        numEntriesRead.inc();
                        numBytesRead.registerSuccessfulValue(dataLength);

                        ReferenceCounted toSend = lh.getDigestManager()
                                .computeDigestAndPackageForSending(entry.getEntryId(),
                                        lh.getLastAddConfirmed(), entry.getLength(),
                                        Unpooled.wrappedBuffer(data, 0, data.length),
                                        lh.getLedgerKey(),
                                        BookieProtocol.FLAG_RECOVERY_ADD);
                        if (replicationThrottle != null) {
                            if (toSend instanceof ByteBuf) {
                                updateAverageEntrySize(((ByteBuf) toSend).readableBytes());
                            } else if (toSend instanceof ByteBufList) {
                                updateAverageEntrySize(((ByteBufList) toSend).readableBytes());
                            }
                        }
                        AtomicInteger numCompleted = new AtomicInteger(0);
                        AtomicBoolean completed = new AtomicBoolean(false);

                        WriteCallback multiWriteCallback = new WriteCallback() {
                            @Override
                            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                                if (rc != BKException.Code.OK) {
                                    LOG.error("BK error writing entry for ledgerId: {}, entryId: {}, bookie: {}",
                                            ledgerId, entryId, addr, BKException.create(rc));
                                    if (completed.compareAndSet(false, true)) {
                                        ledgerFragmentMcb.processResult(rc, null, null);
                                    }
                                } else {
                                    numEntriesWritten.inc();
                                    if (ctx instanceof Long) {
                                        numBytesWritten.registerSuccessfulValue((Long) ctx);
                                    }
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Success writing ledger id {}, entry id {} to a new bookie {}!",
                                                ledgerId, entryId, addr);
                                    }
                                    if (numCompleted.incrementAndGet() == newBookies.size()
                                            && completed.compareAndSet(false, true)) {
                                        ledgerFragmentMcb.processResult(rc, null, null);
                                    }
                                }
                            }
                        };

                        for (BookieId newBookie : newBookies) {
                            long startWriteEntryTime = MathUtils.nowInNano();
                            bkc.getBookieClient().addEntry(newBookie, lh.getId(),
                                    lh.getLedgerKey(), entry.getEntryId(), toSend,
                                    multiWriteCallback, dataLength, BookieProtocol.FLAG_RECOVERY_ADD,
                                    false, WriteFlag.NONE);
                            writeDataLatency.registerSuccessfulEvent(
                                    MathUtils.elapsedNanos(startWriteEntryTime), TimeUnit.NANOSECONDS);
                        }
                        toSend.release();
                    }
                    if (lastEntryId != endEntryId) {
                        try {
                            batchRecoverLedgerFragmentEntry(lastEntryId + 1, endEntryId, lh,
                                    ledgerFragmentMcb, newBookies, onReadEntryFailureCallback);
                        } catch (InterruptedException e) {
                            int remainingEntries = (int) (endEntryId - lastEntryId);
                            for (int i = 0; i < remainingEntries; i++) {
                                ledgerFragmentMcb.processResult(BKException.Code.InterruptedException, null, null);
                            }
                        }
                    }
                }
            }, null);
    }

    private void updateAverageEntrySize(int toSendSize) {
        averageEntrySize.updateAndGet(value -> (int) (value * AVERAGE_ENTRY_SIZE_RATIO
                + (1 - AVERAGE_ENTRY_SIZE_RATIO) * toSendSize));
    }

    /**
     * Callback for recovery of a single ledger fragment. Once the fragment has
     * had all entries replicated, update the ensemble in zookeeper. Once
     * finished propagate callback up to ledgerFragmentsMcb which should be a
     * multicallback responsible for all fragments in a single ledger
     */
    static class SingleFragmentCallback implements AsyncCallback.VoidCallback {
        final AsyncCallback.VoidCallback ledgerFragmentsMcb;
        final LedgerHandle lh;
        final LedgerManager ledgerManager;
        final long fragmentStartId;
        final Map<BookieId, BookieId> oldBookie2NewBookie;

        SingleFragmentCallback(AsyncCallback.VoidCallback ledgerFragmentsMcb,
                               LedgerHandle lh, LedgerManager ledgerManager, long fragmentStartId,
                               Map<BookieId, BookieId> oldBookie2NewBookie) {
            this.ledgerFragmentsMcb = ledgerFragmentsMcb;
            this.lh = lh;
            this.ledgerManager = ledgerManager;
            this.fragmentStartId = fragmentStartId;
            this.oldBookie2NewBookie = oldBookie2NewBookie;
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != BKException.Code.OK) {
                LOG.error("BK error replicating ledger fragments for ledger: "
                        + lh.getId(), BKException.create(rc));
                ledgerFragmentsMcb.processResult(rc, null, null);
                return;
            }
            updateEnsembleInfo(ledgerManager, ledgerFragmentsMcb, fragmentStartId, lh, oldBookie2NewBookie);
        }
    }

    /**
     * Updates the ensemble with newBookie and notify the ensembleUpdatedCb.
     */
    private static void updateEnsembleInfo(
            LedgerManager ledgerManager, AsyncCallback.VoidCallback ensembleUpdatedCb, long fragmentStartId,
            LedgerHandle lh, Map<BookieId, BookieId> oldBookie2NewBookie) {

        MetadataUpdateLoop updateLoop = new MetadataUpdateLoop(
                ledgerManager,
                lh.getId(),
                lh::getVersionedLedgerMetadata,
                (metadata) -> {
                    // returns true if any of old bookies exist in ensemble
                    List<BookieId> ensemble = metadata.getAllEnsembles().get(fragmentStartId);
                    return oldBookie2NewBookie.keySet().stream().anyMatch(ensemble::contains);
                },
                (currentMetadata) -> {
                    // replace all old bookies with new bookies in ensemble
                    List<BookieId> newEnsemble = currentMetadata.getAllEnsembles().get(fragmentStartId)
                        .stream().map((bookie) -> oldBookie2NewBookie.getOrDefault(bookie, bookie))
                        .collect(Collectors.toList());
                    return LedgerMetadataBuilder.from(currentMetadata)
                        .replaceEnsembleEntry(fragmentStartId, newEnsemble).build();
                },
                lh::setLedgerMetadata);

        updateLoop.run().whenComplete((result, ex) -> {
                if (ex == null) {
                    LOG.info("Updated ZK to point ledger fragments"
                             + " from old bookies to new bookies: {}", oldBookie2NewBookie);

                    ensembleUpdatedCb.processResult(BKException.Code.OK, null, null);
                } else {
                    LOG.error("Error updating ledger config metadata for ledgerId {}", lh.getId(), ex);

                    ensembleUpdatedCb.processResult(
                            BKException.getExceptionCode(ex, BKException.Code.UnexpectedConditionException),
                            null, null);
                }
            });
    }

    static class Throttler {
        private final RateLimiter rateLimiter;

        Throttler(int throttleBytes) {
            this.rateLimiter = RateLimiter.create(throttleBytes);
        }

        // reset rate of limiter before compact one entry log file
        void resetRate(int throttleBytes) {
            this.rateLimiter.setRate(throttleBytes);
        }

        // get rate of limiter for unit test
        double getRate() {
            return this.rateLimiter.getRate();
        }

        // acquire. if bybytes: bytes of this entry; if byentries: 1.
        void acquire(int permits) {
            rateLimiter.acquire(permits);
        }
    }
}
