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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Garbage collector implementation using scan and compare.
 *
 * <p>
 * Garbage collection is processed as below:
 * <ul>
 * <li> fetch all existing ledgers from zookeeper or metastore according to
 * the LedgerManager, called <b>globalActiveLedgers</b>
 * <li> fetch all active ledgers from bookie server, said <b>bkActiveLedgers</b>
 * <li> loop over <b>bkActiveLedgers</b> to find those ledgers that are not in
 * <b>globalActiveLedgers</b>, do garbage collection on them.
 * </ul>
 * </p>
 */
public class ScanAndCompareGarbageCollector implements GarbageCollector {

    static final Logger LOG = LoggerFactory.getLogger(ScanAndCompareGarbageCollector.class);

    private final LedgerManager ledgerManager;
    private final CompactableLedgerStorage ledgerStorage;
    private final ServerConfiguration conf;
    private final BookieId selfBookieAddress;
    private boolean enableGcOverReplicatedLedger;
    private final long gcOverReplicatedLedgerIntervalMillis;
    private long lastOverReplicatedLedgerGcTimeMillis;
    private final boolean verifyMetadataOnGc;
    private int activeLedgerCounter;
    private StatsLogger statsLogger;
    private final int maxConcurrentRequests;

    public ScanAndCompareGarbageCollector(LedgerManager ledgerManager, CompactableLedgerStorage ledgerStorage,
            ServerConfiguration conf, StatsLogger statsLogger) throws IOException {
        this.ledgerManager = ledgerManager;
        this.ledgerStorage = ledgerStorage;
        this.conf = conf;
        this.statsLogger = statsLogger;
        this.selfBookieAddress = BookieImpl.getBookieId(conf);

        this.gcOverReplicatedLedgerIntervalMillis = conf.getGcOverreplicatedLedgerWaitTimeMillis();
        this.lastOverReplicatedLedgerGcTimeMillis = System.currentTimeMillis();
        if (gcOverReplicatedLedgerIntervalMillis > 0) {
            this.enableGcOverReplicatedLedger = true;
        }
        this.maxConcurrentRequests = conf.getGcOverreplicatedLedgerMaxConcurrentRequests();
        LOG.info("Over Replicated Ledger Deletion : enabled={}, interval={}, maxConcurrentRequests={}",
                enableGcOverReplicatedLedger, gcOverReplicatedLedgerIntervalMillis, maxConcurrentRequests);

        verifyMetadataOnGc = conf.getVerifyMetadataOnGC();

        this.activeLedgerCounter = 0;
    }

    public int getNumActiveLedgers() {
        return activeLedgerCounter;
    }

    /**
     * 垃圾回收主流程：遍历本地和元数据中的Ledger，清理不需要的Ledger。
     * @param garbageCleaner 执行具体清理操作的实现
     */
    @Override
    public void gc(GarbageCleaner garbageCleaner) {
        // 首先检查 ledgerManager 是否存在，
        // 如果为 null 说明 Bookie 尚未连接元数据存储，此时跳过 GC，避免异常
        if (null == ledgerManager) {
            // 若 ledger manager 为 null，说明 bookie 未启动或未连接 metadata store，
            // 此时跳过垃圾回收
            return;
        }

        try {
            // 获取本地 bookie 上所有活跃（active）的 ledger（范围：从0到无穷大）
            NavigableSet<Long> bkActiveLedgers = Sets.newTreeSet(ledgerStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
            this.activeLedgerCounter = bkActiveLedgers.size(); // 活跃 ledger 数量统计

            // 判断是否需要清理超副本账本：enableGcOverReplicatedLedger 打开且距离上次超副本 GC 已超过配置时间
            long curTime = System.currentTimeMillis();
            boolean checkOverReplicatedLedgers = (enableGcOverReplicatedLedger &&
                    curTime - lastOverReplicatedLedgerGcTimeMillis > gcOverReplicatedLedgerIntervalMillis);

            if (checkOverReplicatedLedgers) {
                LOG.info("Start removing over-replicated ledgers. activeLedgerCounter={}", activeLedgerCounter);

                // 移除本地多余副本的 ledger
                Set<Long> overReplicatedLedgers = removeOverReplicatedledgers(bkActiveLedgers, garbageCleaner);

                if (overReplicatedLedgers.isEmpty()) {
                    LOG.info("No over-replicated ledgers found.");
                } else {
                    LOG.info("Removed over-replicated ledgers: {}", overReplicatedLedgers);
                }

                // 刷新上次超副本 GC 操作时间
                lastOverReplicatedLedgerGcTimeMillis = System.currentTimeMillis();
            }

            // ========== 开始遍历元数据存储上的所有 Ledger ==========
            long zkOpTimeoutMs = this.conf.getZkTimeout() * 2; // 元数据超时为默认两倍
            LedgerRangeIterator ledgerRangeIterator = ledgerManager.getLedgerRanges(zkOpTimeoutMs);

            Set<Long> ledgersInMetadata = null; // 每次遍历一段区间的 ledger
            long start;
            long end = -1;
            boolean done = false;
            AtomicBoolean isBookieInEnsembles = new AtomicBoolean(false);
            Versioned<LedgerMetadata> metadata = null;

            while (!done) {
                // 计算本轮区间起点
                start = end + 1;

                // 判断是否还有下一个 LedgerRange
                if (ledgerRangeIterator.hasNext()) {
                    LedgerRange lRange = ledgerRangeIterator.next();
                    ledgersInMetadata = lRange.getLedgers(); // 该区间所有 ledger（元数据中的）
                    end = lRange.end(); // 区间终止 ledgerId
                } else {
                    // 没有下一个区间了
                    ledgersInMetadata = new TreeSet<>(); // 空集合
                    end = Long.MAX_VALUE; // 终止全局遍历
                    done = true;
                }

                // 获取本地 bookie 上当前区间的活跃 Ledger
                Iterable<Long> subBkActiveLedgers = bkActiveLedgers.subSet(start, true, end, true);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Active in metadata {}, Active in bookie {}", ledgersInMetadata, subBkActiveLedgers);
                }

                for (Long bkLid : subBkActiveLedgers) {
                    // 若本地 ledger 不在元数据 ledger 列表中
                    if (!ledgersInMetadata.contains(bkLid)) {
                        // 如果开启元数据校验，则还要进一步比对
                        if (verifyMetadataOnGc) {
                            isBookieInEnsembles.set(false);
                            metadata = null;
                            int rc = BKException.Code.OK;
                            try {
                                // 从元数据存储读取账本元数据
                                metadata = result(ledgerManager.readLedgerMetadata(bkLid), zkOpTimeoutMs, TimeUnit.MILLISECONDS);
                            } catch (BKException | TimeoutException e) {
                                if (e instanceof BKException) {
                                    rc = ((BKException) e).getCode();
                                } else {
                                    // 超时时输出警告，跳过此 ledger
                                    LOG.warn("Time-out while fetching metadata for Ledger {} : {}.", bkLid, e.getMessage());
                                    continue;
                                }
                            }

                            // 检查本地 bookie 是否在任意 ledger segment 的 ensemble 列表中
                            // 如果本地仍在 ensemble 中，则跳过清理
                            if (metadata != null && metadata.getValue() != null) {
                                metadata.getValue().getAllEnsembles().forEach((entryId, ensembles) -> {
                                    if (ensembles != null && ensembles.contains(selfBookieAddress)) {
                                        isBookieInEnsembles.set(true); // 本节点还在该 ledger 的副本列表中
                                    }
                                });
                                if (isBookieInEnsembles.get()) {
                                    continue; // 本节点还被副本引用，不能清理
                                }
                                // 如果元数据异常，但不是“账本不存在”，则也跳过
                            } else if (rc != BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                                LOG.warn("Ledger {} Missing in metadata list, but ledgerManager returned rc: {}.", bkLid, rc);
                                continue;
                            }
                        }
                        // 确认本地 ledger 已无法从元数据找到且没有副本关系，可以清理
                        garbageCleaner.clean(bkLid);
                    }
                }
            }
        } catch (Throwable t) {
            // 捕获所有异常（包含运行期异常），输出日志，下次 GC 时重试
            LOG.warn("Exception when iterating over the metadata", t);
        }
    }


    private Set<Long> removeOverReplicatedledgers(Set<Long> bkActiveledgers, final GarbageCleaner garbageCleaner)
            throws Exception {
        final Set<Long> overReplicatedLedgers = Sets.newHashSet();
        final Semaphore semaphore = new Semaphore(this.maxConcurrentRequests);
        final CountDownLatch latch = new CountDownLatch(bkActiveledgers.size());
        // instantiate zookeeper client to initialize ledger manager

        @Cleanup
        MetadataBookieDriver metadataDriver = instantiateMetadataDriver(conf, statsLogger);

        @Cleanup
        LedgerManagerFactory lmf = metadataDriver.getLedgerManagerFactory();

        @Cleanup
        LedgerUnderreplicationManager lum = lmf.newLedgerUnderreplicationManager();

        for (final Long ledgerId : bkActiveledgers) {
            try {
                // check ledger ensembles before creating lock nodes.
                // this is to reduce the number of lock node creations and deletions in ZK.
                // the ensemble check is done again after the lock node is created.
                Versioned<LedgerMetadata> preCheckMetadata = ledgerManager.readLedgerMetadata(ledgerId).get();
                if (!isNotBookieIncludedInLedgerEnsembles(preCheckMetadata)) {
                    latch.countDown();
                    continue;
                }
            } catch (Throwable t) {
                if (!(t.getCause() instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException)) {
                    LOG.warn("Failed to get metadata for ledger {}. {}: {}",
                            ledgerId, t.getClass().getName(), t.getMessage());
                }
                latch.countDown();
                continue;
            }

            try {
                // check if the ledger is being replicated already by the replication worker
                if (lum.isLedgerBeingReplicated(ledgerId)) {
                    latch.countDown();
                    continue;
                }
                // we try to acquire the underreplicated ledger lock to not let the bookie replicate the ledger that is
                // already being checked for deletion, since that might change the ledger ensemble to include the
                // current bookie again and, in that case, we cannot remove the ledger from local storage
                lum.acquireUnderreplicatedLedger(ledgerId);
                semaphore.acquire();
                ledgerManager.readLedgerMetadata(ledgerId)
                    .whenComplete((metadata, exception) -> {
                            try {
                                if (exception == null) {
                                    if (isNotBookieIncludedInLedgerEnsembles(metadata)) {
                                        // this bookie is not supposed to have this ledger,
                                        // thus we can delete this ledger now
                                        overReplicatedLedgers.add(ledgerId);
                                        garbageCleaner.clean(ledgerId);
                                    }
                                } else if (!(exception
                                        instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException)) {
                                    LOG.warn("Failed to get metadata for ledger {}. {}: {}",
                                            ledgerId, exception.getClass().getName(), exception.getMessage());
                                }
                            } finally {
                                semaphore.release();
                                latch.countDown();
                                try {
                                    lum.releaseUnderreplicatedLedger(ledgerId);
                                } catch (Throwable t) {
                                    LOG.error("Exception when removing underreplicated lock for ledger {}",
                                              ledgerId, t);
                                }
                            }
                        });
            } catch (Throwable t) {
                LOG.error("Exception when iterating through the ledgers to check for over-replication", t);
                latch.countDown();
            }
        }
        latch.await();
        bkActiveledgers.removeAll(overReplicatedLedgers);
        return overReplicatedLedgers;
    }

    private static MetadataBookieDriver instantiateMetadataDriver(ServerConfiguration conf, StatsLogger statsLogger)
            throws BookieException {
        try {
            String metadataServiceUriStr = conf.getMetadataServiceUri();
            MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(URI.create(metadataServiceUriStr));
            driver.initialize(
                    conf,
                    statsLogger);
            return driver;
        } catch (MetadataException me) {
            throw new BookieException.MetadataStoreException("Failed to initialize metadata bookie driver", me);
        } catch (ConfigurationException e) {
            throw new BookieException.BookieIllegalOpException(e);
        }
    }

    private boolean isNotBookieIncludedInLedgerEnsembles(Versioned<LedgerMetadata> metadata) {
        // do not delete a ledger that is not closed, since the ensemble might
        // change again and include the current bookie while we are deleting it
        if (!metadata.getValue().isClosed()) {
            return false;
        }

        SortedMap<Long, ? extends List<BookieId>> ensembles =
                metadata.getValue().getAllEnsembles();
        for (List<BookieId> ensemble : ensembles.values()) {
            // check if this bookie is supposed to have this ledger
            if (ensemble.contains(selfBookieAddress)) {
                return false;
            }
        }

        return true;
    }
}
