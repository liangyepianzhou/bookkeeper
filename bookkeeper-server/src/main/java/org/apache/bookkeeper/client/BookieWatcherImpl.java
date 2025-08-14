/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NEW_ENSEMBLE_TIME;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.REPLACE_BOOKIE_TIME;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WATCHER_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CREATE_OP;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookKeeperServerStats;
import org.apache.bookkeeper.client.BKException.BKInterruptedException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BKException.MetaStoreException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * This class is responsible for maintaining a consistent view of what bookies
 * are available by reading Zookeeper (and setting watches on the bookie nodes).
 * When a bookie fails, the other parts of the code turn to this class to find a
 * replacement
 *
 *  一、整体定位：BookieWatcherImpl 是什么？
 * BookieWatcherImpl 是 BookKeeper 客户端中用于感知 Bookie 节点状态变化的“观察者”模块。
 *
 * ✅ 主要职责：
 * 监听 ZooKeeper 中的 Bookie 列表变化（读写/只读节点）。
 * 维护当前可用的 Bookie 集合（writableBookies, readOnlyBookies）。
 * 为 Ledger 创建和 Bookie 替换提供健康节点列表。
 * 支持 Bookie “隔离”机制（quarantine）：临时排除有问题的 Bookie。
 * 与 EnsemblePlacementPolicy 协同工作，实现智能选节点策略。
 *
 * | 字段 | 类型 | 说明 |
 * |------|------|------|
 * | `conf` | `ClientConfiguration` | 客户端配置 |
 * | `registrationClient` | `RegistrationClient` | 与 ZooKeeper 交互，获取 Bookie 注册信息 |
 * | `placementPolicy` | `EnsemblePlacementPolicy` | 节点选择策略（如机架感知） |
 * | `bookieAddressResolver` | `BookieAddressResolver` | 解析 Bookie ID 到网络地址 |
 * | `writableBookies` / `readOnlyBookies` | `volatile Set<BookieId>` | 当前可写/只读 Bookie 列表（线程安全） |
 * | `quarantinedBookies` | `Cache<BookieId, Boolean>` | 被“隔离”的 Bookie 缓存，超时后自动释放 |
 * | `newEnsembleTimer`, `replaceBookieTimer` | `OpStatsLogger` | 性能统计：创建/替换 Ensemble 的耗时 |
 * | `ensembleNotAdheringToPlacementPolicy` | `Counter` | 统计违反放置策略的次数 |
 */
@StatsDoc(
    name = WATCHER_SCOPE,
    help = "Bookie watcher related stats"
)
@Slf4j
class BookieWatcherImpl implements BookieWatcher {

    private static final Function<Throwable, BKException> EXCEPTION_FUNC = cause -> {
        if (cause instanceof BKException) {
            log.error("Failed to get bookie list : ", cause);
            return (BKException) cause;
        } else if (cause instanceof InterruptedException) {
            log.error("Interrupted reading bookie list : ", cause);
            return new BKInterruptedException();
        } else {
            MetaStoreException mse = new MetaStoreException(cause);
            return mse;
        }
    };

    private final ClientConfiguration conf;
    private final RegistrationClient registrationClient;
    private final EnsemblePlacementPolicy placementPolicy;
    @StatsDoc(
        name = NEW_ENSEMBLE_TIME,
        help = "operation stats of new ensembles",
        parent = CREATE_OP
    )
    private final OpStatsLogger newEnsembleTimer;
    @StatsDoc(
        name = REPLACE_BOOKIE_TIME,
        help = "operation stats of replacing bookie in an ensemble"
    )
    private final OpStatsLogger replaceBookieTimer;
    @StatsDoc(
            name = ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNT,
            help = "total number of newEnsemble/replaceBookie operations failed to adhere"
            + " EnsemblePlacementPolicy"
    )
    private final Counter ensembleNotAdheringToPlacementPolicy;

    // Bookies that will not be preferred to be chosen in a new ensemble
    final Cache<BookieId, Boolean> quarantinedBookies;

    private volatile Set<BookieId> writableBookies = Collections.emptySet();
    private volatile Set<BookieId> readOnlyBookies = Collections.emptySet();

    private CompletableFuture<?> initialWritableBookiesFuture = null;
    private CompletableFuture<?> initialReadonlyBookiesFuture = null;

    private final BookieAddressResolver bookieAddressResolver;

    public BookieWatcherImpl(ClientConfiguration conf,
                             EnsemblePlacementPolicy placementPolicy,
                             RegistrationClient registrationClient,
                             BookieAddressResolver bookieAddressResolver,
                             StatsLogger statsLogger)  {
        this.conf = conf;
        this.bookieAddressResolver = bookieAddressResolver;
        this.placementPolicy = placementPolicy;
        this.registrationClient = registrationClient;
        this.quarantinedBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieQuarantineTimeSeconds(), TimeUnit.SECONDS)
                .removalListener(new RemovalListener<BookieId, Boolean>() {

                    @Override
                    public void onRemoval(RemovalNotification<BookieId, Boolean> bookie) {
                        log.info("Bookie {} is no longer quarantined", bookie.getKey());
                    }

                }).build();
        this.newEnsembleTimer = statsLogger.getOpStatsLogger(NEW_ENSEMBLE_TIME);
        this.replaceBookieTimer = statsLogger.getOpStatsLogger(REPLACE_BOOKIE_TIME);
        this.ensembleNotAdheringToPlacementPolicy = statsLogger
                .getCounter(BookKeeperServerStats.ENSEMBLE_NOT_ADHERING_TO_PLACEMENT_POLICY_COUNT);
    }

    @Override
    public Set<BookieId> getBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getWritableBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    @Override
    public Set<BookieId> getAllBookies() throws BKException {
        try {
            return FutureUtils.result(registrationClient.getAllBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    @Override
    public BookieAddressResolver getBookieAddressResolver() {
        return this.bookieAddressResolver;
    }

    @Override
    public Set<BookieId> getReadOnlyBookies()
            throws BKException {
        try {
            return FutureUtils.result(registrationClient.getReadOnlyBookies(), EXCEPTION_FUNC).getValue();
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }

    /**
     * Determine if a bookie should be considered unavailable.
     * This does not require a network call because this class
     * maintains a current view of readonly and writable bookies.
     * An unavailable bookie is one that is neither read only nor
     * writable.
     *
     * @param id
     *          Bookie to check
     * @return whether or not the given bookie is unavailable
     */
    @Override
    public boolean isBookieUnavailable(BookieId id) {
        return !readOnlyBookies.contains(id) && !writableBookies.contains(id);
    }

    // 此回调方法已经不在 ZooKeeper 的线程中执行
    private synchronized void processWritableBookiesChanged(Set<BookieId> newBookieAddrs) {
        // 在 ZooKeeper 回调线程之外更新 watcher，以避免死锁。
        // 因为其他组件可能正在执行阻塞的 ZooKeeper 操作。
        this.writableBookies = newBookieAddrs;
        placementPolicy.onClusterChanged(newBookieAddrs, readOnlyBookies);

        // 这里不需要关闭客户端连接，原因如下：
        // a. 故障的 Bookie 会被从集群拓扑中移除，因此不会再被用于新的 ensemble。
        // b. 读取请求的顺序会根据 znode 的可用性重新调整，因此大多数读请求不会发送到这些故障节点。
        // c. 在这里关闭连接只是断开 Channel，但并不会将其从 pcbc（PerChannelBookieClient）映射中移除。
        //    我们其实不需要在此处主动断开连接，因为如果 Bookie 确实宕机了，PCBC 会通过 Netty 的回调自动断开。
        //    如果我们在这里强行断开，反而会引入问题（见 d 点）。
        // d. 如果 Bookie 实际上是存活的，只是由于 ZooKeeper 会话过期导致其 znode 注册短暂异常（flaky），
        //    此时关闭连接会影响请求延迟，造成不必要的性能波动。
        // e. 如果我们希望永久移除一个 BookKeeper 客户端，应该通过监听 "cookies" 列表来实现。

        // 因此，以下代码被注释掉：
        // if (bk.getBookieClient() != null) {
        //     bk.getBookieClient().closeClients(deadBookies);
        // }
    }

    private synchronized void processReadOnlyBookiesChanged(Set<BookieId> readOnlyBookies) {
        this.readOnlyBookies = readOnlyBookies;
        placementPolicy.onClusterChanged(writableBookies, readOnlyBookies);
    }

    /**
     * Blocks until bookies are read from zookeeper, used in the {@link BookKeeper} constructor.
     *
     * @throws BKException when failed to read bookies
     */
    public void initialBlockingBookieRead() throws BKException {

        CompletableFuture<?> writable;
        CompletableFuture<?> readonly;
        synchronized (this) {
            if (initialReadonlyBookiesFuture == null) {
                assert initialWritableBookiesFuture == null;

                writable = this.registrationClient.watchWritableBookies(
                            bookies -> processWritableBookiesChanged(bookies.getValue()));

                readonly = this.registrationClient.watchReadOnlyBookies(
                            bookies -> processReadOnlyBookiesChanged(bookies.getValue()));
                initialWritableBookiesFuture = writable;
                initialReadonlyBookiesFuture = readonly;
            } else {
                writable = initialWritableBookiesFuture;
                readonly = initialReadonlyBookiesFuture;
            }
        }
        try {
            FutureUtils.result(writable, EXCEPTION_FUNC);
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
        try {
            FutureUtils.result(readonly, EXCEPTION_FUNC);
        } catch (BKInterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (Exception e) {
            log.error("Failed getReadOnlyBookies: ", e);
        }
    }

    @Override
    public List<BookieId> newEnsemble(int ensembleSize, int writeQuorumSize,
        int ackQuorumSize, Map<String, byte[]> customMetadata)
            throws BKNotEnoughBookiesException {
        long startTime = MathUtils.nowInNano();
        EnsemblePlacementPolicy.PlacementResult<List<BookieId>> newEnsembleResponse;
        List<BookieId> socketAddresses;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy;
        try {
            Set<BookieId> quarantinedBookiesSet = quarantinedBookies.asMap().keySet();
            newEnsembleResponse = placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                    customMetadata, new HashSet<BookieId>(quarantinedBookiesSet));
            socketAddresses = newEnsembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = newEnsembleResponse.getAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                if (ensembleSize > 1) {
                    log.info("New ensemble: {} is not adhering to Placement Policy. quarantinedBookies: {}",
                            socketAddresses, quarantinedBookiesSet);
                }
            }
            // we try to only get from the healthy bookies first
            newEnsembleTimer.registerSuccessfulEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        } catch (BKNotEnoughBookiesException e) {
            if (log.isDebugEnabled()) {
                log.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            newEnsembleResponse = placementPolicy.newEnsemble(
                    ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata, new HashSet<>());
            socketAddresses = newEnsembleResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = newEnsembleResponse.getAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                log.info("New ensemble: {} is not adhering to Placement Policy", socketAddresses);
            }
            newEnsembleTimer.registerFailedEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        }
        return socketAddresses;
    }

    @Override
    public BookieId replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                             Map<String, byte[]> customMetadata,
                                             List<BookieId> existingBookies, int bookieIdx,
                                             Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        long startTime = MathUtils.nowInNano();
        BookieId addr = existingBookies.get(bookieIdx);
        EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse;
        BookieId socketAddress;
        PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = PlacementPolicyAdherence.FAIL;
        try {
            // we exclude the quarantined bookies also first
            Set<BookieId> excludedBookiesAndQuarantinedBookies = new HashSet<BookieId>(
                    excludeBookies);
            Set<BookieId> quarantinedBookiesSet = quarantinedBookies.asMap().keySet();
            excludedBookiesAndQuarantinedBookies.addAll(quarantinedBookiesSet);
            replaceBookieResponse = placementPolicy.replaceBookie(
                    ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata,
                    existingBookies, addr, excludedBookiesAndQuarantinedBookies);
            socketAddress = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                log.warn(
                        "replaceBookie for bookie: {} in ensemble: {} is not adhering to placement policy and"
                                + " chose {}. excludedBookies {} and quarantinedBookies {}",
                        addr, existingBookies, socketAddress, excludeBookies, quarantinedBookiesSet);
            }
            replaceBookieTimer.registerSuccessfulEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        } catch (BKNotEnoughBookiesException e) {
            if (log.isDebugEnabled()) {
                log.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            replaceBookieResponse = placementPolicy.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize,
                    customMetadata, existingBookies, addr, excludeBookies);
            socketAddress = replaceBookieResponse.getResult();
            isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL) {
                ensembleNotAdheringToPlacementPolicy.inc();
                log.warn(
                        "replaceBookie for bookie: {} in ensemble: {} is not adhering to placement policy and"
                                + " chose {}. excludedBookies {}",
                        addr, existingBookies, socketAddress, excludeBookies);
            }
            replaceBookieTimer.registerFailedEvent(MathUtils.nowInNano() - startTime, TimeUnit.NANOSECONDS);
        }
        return socketAddress;
    }

    /**
     * Quarantine <i>bookie</i> so it will not be preferred to be chosen for new ensembles.
     * @param bookie
     */
    @Override
    public void quarantineBookie(BookieId bookie) {
        if (quarantinedBookies.getIfPresent(bookie) == null) {
            quarantinedBookies.put(bookie, Boolean.TRUE);
            log.warn("Bookie {} has been quarantined because of read/write errors.", bookie);
        }
    }

    @Override
    public void releaseAllQuarantinedBookies(){
        quarantinedBookies.invalidateAll();
    }
}
