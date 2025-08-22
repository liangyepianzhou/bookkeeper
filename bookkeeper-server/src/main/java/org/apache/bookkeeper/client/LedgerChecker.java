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

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to check the complete ledger and finds the UnderReplicated fragments if any.
 *
 * <p>NOTE: This class is tended to be used by this project only. External users should not rely on it directly.
 */
public class LedgerChecker {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerChecker.class);

    public final BookieClient bookieClient;
    public final BookieWatcher bookieWatcher;

    final Semaphore semaphore;

    static class InvalidFragmentException extends Exception {
        private static final long serialVersionUID = 1467201276417062353L;
    }

    /**
     * This will collect all the entry read call backs and finally it will give
     * call back to previous call back API which is waiting for it once it meets
     * the expected call backs from down.
     */
    private class ReadManyEntriesCallback implements ReadEntryCallback {
        AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicLong numEntries;
        final LedgerFragment fragment;
        final GenericCallback<LedgerFragment> cb;

        ReadManyEntriesCallback(long numEntries, LedgerFragment fragment,
                GenericCallback<LedgerFragment> cb) {
            this.numEntries = new AtomicLong(numEntries);
            this.fragment = fragment;
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                ByteBuf buffer, Object ctx) {
            releasePermit();
            if (rc == BKException.Code.OK) {
                if (numEntries.decrementAndGet() == 0
                        && !completed.getAndSet(true)) {
                    cb.operationComplete(rc, fragment);
                }
            } else if (!completed.getAndSet(true)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Read {}:{} from {} failed, the error code: {}", ledgerId, entryId, ctx, rc);
                }
                cb.operationComplete(rc, fragment);
            }
        }
    }

    /**
     * This will collect the bad bookies inside a ledger fragment.
     */
    private static class LedgerFragmentCallback implements GenericCallback<LedgerFragment> {

        private final LedgerFragment fragment;
        private final int bookieIndex;
        // bookie index -> return code
        private final Map<Integer, Integer> badBookies;
        private final AtomicInteger numBookies;
        private final GenericCallback<LedgerFragment> cb;

        LedgerFragmentCallback(LedgerFragment lf,
                               int bookieIndex,
                               GenericCallback<LedgerFragment> cb,
                               Map<Integer, Integer> badBookies,
                               AtomicInteger numBookies) {
            this.fragment = lf;
            this.bookieIndex = bookieIndex;
            this.cb = cb;
            this.badBookies = badBookies;
            this.numBookies = numBookies;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment lf) {
            if (BKException.Code.OK != rc) {
                synchronized (badBookies) {
                    badBookies.put(bookieIndex, rc);
                }
            }
            if (numBookies.decrementAndGet() == 0) {
                if (badBookies.isEmpty()) {
                    cb.operationComplete(BKException.Code.OK, fragment);
                } else {
                    int rcToReturn = BKException.Code.NoBookieAvailableException;
                    for (Map.Entry<Integer, Integer> entry : badBookies.entrySet()) {
                        rcToReturn = entry.getValue();
                        if (entry.getValue() == BKException.Code.ClientClosedException) {
                            break;
                        }
                    }
                    cb.operationComplete(rcToReturn,
                            fragment.subset(badBookies.keySet()));
                }
            }
        }
    }

    public LedgerChecker(BookKeeper bkc) {
        this(bkc.getBookieClient(), bkc.getBookieWatcher());
    }

    public LedgerChecker(BookieClient client, BookieWatcher watcher) {
        this(client, watcher, -1);
    }

    public LedgerChecker(BookKeeper bkc, int inFlightReadEntryNum) {
        this(bkc.getBookieClient(), bkc.getBookieWatcher(), inFlightReadEntryNum);
    }

    public LedgerChecker(BookieClient client, BookieWatcher watcher, int inFlightReadEntryNum) {
        bookieClient = client;
        bookieWatcher = watcher;
        if (inFlightReadEntryNum > 0) {
            semaphore = new Semaphore(inFlightReadEntryNum);
        } else {
            semaphore = null;
        }
    }

    /**
     * Acquires a permit from permit manager,
     * blocking until all are available.
     */
    public void acquirePermit() throws InterruptedException {
        if (null != semaphore) {
            semaphore.acquire(1);
        }
    }

    /**
     * Release a given permit.
     */
    public void releasePermit() {
        if (null != semaphore) {
            semaphore.release();
        }
    }

    /**
     * 校验指定账本片段（LedgerFragment），收集其中存在问题的 bookie。
     * 对片段中的每一个副本（bookie）单独并发检测，最终回调返回结果。
     *
     * @param fragment      需要校验的账本片段
     * @param cb            校验完成后的回调（GenericCallback<LedgerFragment>），用于返回结果
     * @param percentageOfLedgerFragmentToBeVerified 校验采样比例（提升性能，减少全量校验开销）
     * @throws InvalidFragmentException 片段结构异常/无效时抛出
     * @throws BKException              BookKeeper体系相关异常
     * @throws InterruptedException     线程中断异常
     */
    private void verifyLedgerFragment(LedgerFragment fragment,
                                      GenericCallback<LedgerFragment> cb,
                                      Long percentageOfLedgerFragmentToBeVerified)
            throws InvalidFragmentException, BKException, InterruptedException {
        // 获取待校验的 bookie 副本索引集合（即该片段的分布副本）
        Set<Integer> bookiesToCheck = fragment.getBookiesIndexes();
        if (bookiesToCheck.isEmpty()) {
            // 如果没有副本，立即返回成功
            cb.operationComplete(BKException.Code.OK, fragment);
            return;
        }

        // 统计需校验 bookie 总数，用于异步合并结果
        AtomicInteger numBookies = new AtomicInteger(bookiesToCheck.size());
        // 收集发现的坏 bookie（副本索引 -> 错误码）
        Map<Integer, Integer> badBookies = new HashMap<Integer, Integer>();

        // 并发处理每一个 bookie 副本的片段校验
        for (Integer bookieIndex : bookiesToCheck) {
            // 对每个副本都创建一个异步回调，收集单副本检验结果，合并到 badBookies
            LedgerFragmentCallback lfCb = new LedgerFragmentCallback(
                    fragment, bookieIndex, cb, badBookies, numBookies);
            // 实际发起对每个副本的详细校验（比如读 entry 检查数据完整性等）
            verifyLedgerFragment(fragment, bookieIndex, lfCb, percentageOfLedgerFragmentToBeVerified);
        }
    }

    /**
     * 校验 ledger fragment 在指定 bookie 上的内容完整性。
     * 支持自动采样、多副本分支、坏副本快速跳过等。
     *
     * @param fragment  当前需要检查的账本片段
     * @param bookieIndex  当前片段bookie副本索引
     * @param cb  回调，用于返回校验结果或异常
     * @param percentageOfLedgerFragmentToBeVerified 采样校验百分比
     * @throws InvalidFragmentException  片段数据异常
     * @throws InterruptedException      线程中断异常
     */
    private void verifyLedgerFragment(LedgerFragment fragment,
                                      int bookieIndex,
                                      GenericCallback<LedgerFragment> cb,
                                      long percentageOfLedgerFragmentToBeVerified)
            throws InvalidFragmentException, InterruptedException {
        long firstStored = fragment.getFirstStoredEntryId(bookieIndex); // 该 bookie 上存储的首条 entryId
        long lastStored = fragment.getLastStoredEntryId(bookieIndex);   // 该 bookie 上存储的末条 entryId

        BookieId bookie = fragment.getAddress(bookieIndex); // 当前副本 bookie 的物理地址
        if (null == bookie) {
            throw new InvalidFragmentException();
        }

        // fragment不在该bookie上：首尾都为无效ID
        if (firstStored == LedgerHandle.INVALID_ENTRY_ID) {
            if (lastStored != LedgerHandle.INVALID_ENTRY_ID) {
                throw new InvalidFragmentException();
            }
            // 该 bookie 注册为不可用，直接标记跳过
            if (bookieWatcher.isBookieUnavailable(fragment.getAddress(bookieIndex))) {
                cb.operationComplete(BKException.Code.BookieHandleNotAvailableException, fragment);
            } else {
                // 副本正常，但没落在该 bookie 上，认为 OK
                cb.operationComplete(BKException.Code.OK, fragment);
            }
            // 副本应该在该 bookie 上，但该节点不可用，直接返回坏副本
        } else if (bookieWatcher.isBookieUnavailable(fragment.getAddress(bookieIndex))) {
            cb.operationComplete(BKException.Code.BookieHandleNotAvailableException, fragment);
            // 该 fragment 只包含一个 entry，只需校验这一个 entry 即可
        } else if (firstStored == lastStored) {
            acquirePermit(); // 并发控制
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(1, fragment, cb);
            bookieClient.readEntry(bookie, fragment.getLedgerId(), firstStored,
                    manycb, bookie, BookieProtocol.FLAG_NONE);
        } else {
            // 多 entry，如 lastStored < firstStored，参数不正确
            if (lastStored <= firstStored) {
                cb.operationComplete(Code.IncorrectParameterException, null);
                return;
            }

            long lengthOfLedgerFragment = lastStored - firstStored + 1;

            // 采样校验条数，百分比到实际整数条数
            int numberOfEntriesToBeVerified =
                    (int) (lengthOfLedgerFragment * (percentageOfLedgerFragmentToBeVerified / 100.0));

            TreeSet<Long> entriesToBeVerified = new TreeSet<Long>();

            if (numberOfEntriesToBeVerified < lengthOfLedgerFragment) {
                // 采样校验，只验证头、尾及分布采样点，覆盖面均匀分散
                if (numberOfEntriesToBeVerified > 0) {
                    int lengthOfBucket = (int) (lengthOfLedgerFragment / numberOfEntriesToBeVerified);
                    for (long index = firstStored;
                         index < (lastStored - lengthOfBucket - 1);
                         index += lengthOfBucket) {
                        // 每个区间取一个随机 entry 作为样本点
                        long potentialEntryId = ThreadLocalRandom.current().nextInt((lengthOfBucket)) + index;
                        if (fragment.isStoredEntryId(potentialEntryId, bookieIndex)) {
                            entriesToBeVerified.add(potentialEntryId);
                        }
                    }
                }
                // 校验片段的头和尾，保证边界数据一直被验证
                entriesToBeVerified.add(firstStored);
                entriesToBeVerified.add(lastStored);
            } else {
                // fragment较短，直接校验所有 entry
                while (firstStored <= lastStored) {
                    if (fragment.isStoredEntryId(firstStored, bookieIndex)) {
                        entriesToBeVerified.add(firstStored);
                    }
                    firstStored++;
                }
            }
            // 发起多个并发异步读，使用 ReadManyEntriesCallback 合并结果
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(entriesToBeVerified.size(), fragment, cb);
            for (Long entryID: entriesToBeVerified) {
                acquirePermit();
                bookieClient.readEntry(bookie, fragment.getLedgerId(), entryID, manycb, bookie,
                        BookieProtocol.FLAG_NONE);
            }
        }
    }


    /**
     * Callback for checking whether an entry exists or not.
     * It is used to differentiate the cases where it has been written
     * but now cannot be read, and where it never has been written.
     */
    private class EntryExistsCallback implements ReadEntryCallback {
        AtomicBoolean entryMayExist = new AtomicBoolean(false);
        final AtomicInteger numReads;
        final GenericCallback<Boolean> cb;

        EntryExistsCallback(int numReads,
                            GenericCallback<Boolean> cb) {
            this.numReads = new AtomicInteger(numReads);
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                                      ByteBuf buffer, Object ctx) {
            releasePermit();
            if (BKException.Code.NoSuchEntryException != rc && BKException.Code.NoSuchLedgerExistsException != rc
                    && BKException.Code.NoSuchLedgerExistsOnMetadataServerException != rc) {
                entryMayExist.set(true);
            }

            if (numReads.decrementAndGet() == 0) {
                cb.operationComplete(rc, entryMayExist.get());
            }
        }
    }

    /**
     * This will collect all the fragment read call backs and finally it will
     * give call back to above call back API which is waiting for it once it
     * meets the expected call backs from down.
     */
    private static class FullLedgerCallback implements
            GenericCallback<LedgerFragment> {
        final Set<LedgerFragment> badFragments;
        final AtomicLong numFragments;
        final GenericCallback<Set<LedgerFragment>> cb;

        FullLedgerCallback(long numFragments,
                GenericCallback<Set<LedgerFragment>> cb) {
            badFragments = new LinkedHashSet<>();
            this.numFragments = new AtomicLong(numFragments);
            this.cb = cb;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment result) {
            if (rc == BKException.Code.ClientClosedException) {
                cb.operationComplete(BKException.Code.ClientClosedException, badFragments);
                return;
            } else if (rc != BKException.Code.OK) {
                badFragments.add(result);
            }
            if (numFragments.decrementAndGet() == 0) {
                cb.operationComplete(BKException.Code.OK, badFragments);
            }
        }
    }

    /**
     * Check that all the fragments in the passed in ledger, and report those
     * which are missing.
     */
    public void checkLedger(final LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb) {
        checkLedger(lh, cb, 0L);
    }

    /**
     * 检查指定账本的所有分片（LedgerFragment），根据 ensemble 切分并回调结果。
     * 最后一个分片（segment）的校验逻辑特殊，需区分账本关闭/未关闭及是否有写入数据。
     *
     * @param lh 账本句柄（LedgerHandle）
     * @param cb 校验完后的回调（GenericCallback<Set<LedgerFragment>>）
     * @param percentageOfLedgerFragmentToBeVerified 本次校验的分片采样百分比
     */
    public void checkLedger(final LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb,
                            long percentageOfLedgerFragmentToBeVerified) {
        // 保存所有分片（片段），按 ensemble（每次账本副本分布变更）切分
        final Set<LedgerFragment> fragments = new LinkedHashSet<>();

        Long curEntryId = null;
        List<BookieId> curEnsemble = null;
        // 遍历账本的每个 ensemble（副本布局变化起始 entryId，与bookie列表）
        for (Map.Entry<Long, ? extends List<BookieId>> e : lh
                .getLedgerMetadata().getAllEnsembles().entrySet()) {
            if (curEntryId != null) {
                // 获得 ensemble 分片中的所有书写副本索引
                Set<Integer> bookieIndexes = new HashSet<Integer>();
                for (int i = 0; i < curEnsemble.size(); i++) {
                    bookieIndexes.add(i);
                }
                // 按当前 fragment 起止 entryId（上一ensemble起点，当前ensemble前一entry），和副本书写分布， 创建LedgerFragment
                fragments.add(new LedgerFragment(lh, curEntryId,
                        e.getKey() - 1, bookieIndexes));
            }
            // 更新当前ensemble起点和副本信息
            curEntryId = e.getKey();
            curEnsemble = e.getValue();
        }

        /*
         * 对账本最后一个分片（segment）进行特殊处理。
         * - 如果账本已关闭，典型场景，可直接校验最后分片（即便没写过数据）
         * - 如果账本未关闭，且 lastAddConfirmed > 最后分片的起始 entryId，可直接校验
         * - 如果账本未关闭且 lastAddConfirmed 小于等于最后分片起始 entryId，可能没有数据，这时必须实际请求所有 bookie 读 entry
         *   - 若 bookie 返回 NoSuchEntry，说明没有数据写入，可跳过
         *   - 若 bookie 返回数据，说明至少有写入，应正常校验该分片
         */
        if (curEntryId != null) {
            long lastEntry = lh.getLastAddConfirmed();

            // 未关闭且没有写够 entry，则用分片起点补齐（特殊场景）
            if (!lh.isClosed() && lastEntry < curEntryId) {
                lastEntry = curEntryId;
            }

            Set<Integer> bookieIndexes = new HashSet<Integer>();
            for (int i = 0; i < curEnsemble.size(); i++) {
                bookieIndexes.add(i);
            }
            final LedgerFragment lastLedgerFragment = new LedgerFragment(lh, curEntryId,
                    lastEntry, bookieIndexes);

            // 如果分片只有一个 entry，且 lastConfirmedEntry == 起始 entry，需要特殊检查（确定有无数据写入）
            if (curEntryId == lastEntry) {
                final long entryToRead = curEntryId;

                // 用 CompletableFuture 做异步流程衔接
                final CompletableFuture<Void> future = new CompletableFuture<>();
                future.whenCompleteAsync((re, ex) -> {
                    // 校验分片集合（回调），用于实际逻辑处理
                    checkFragments(fragments, cb, percentageOfLedgerFragmentToBeVerified);
                });

                // EntryExistsCallback 会收集多个副本的读结果，只要有一个返回说明该 entry 已经写过，则该分片必须加到校验集合中
                final EntryExistsCallback eecb = new EntryExistsCallback(
                        lh.getLedgerMetadata().getWriteQuorumSize(),
                        new GenericCallback<Boolean>() {
                            @Override
                            public void operationComplete(int rc, Boolean result) {
                                if (result) {
                                    fragments.add(lastLedgerFragment);
                                }
                                future.complete(null); // 都收集完后，回调主流程
                            }
                        });

                DistributionSchedule ds = lh.getDistributionSchedule();
                for (int i = 0; i < ds.getWriteQuorumSize(); i++) {
                    try {
                        acquirePermit(); // 控制并发数
                        BookieId addr = curEnsemble.get(ds.getWriteSetBookieIndex(entryToRead, i));
                        // 向每个副本请求读 entry，异步收集读结果
                        bookieClient.readEntry(addr, lh.getId(), entryToRead,
                                eecb, null, BookieProtocol.FLAG_NONE);
                    } catch (InterruptedException e) {
                        LOG.error("检查 entry {} 时线程被中断", entryToRead, e);
                    }
                }
                return;
            } else {
                // 有数据可直接校验
                fragments.add(lastLedgerFragment);
            }
        }

        // 校验所有分片（进行采样、标记等实际日志处理）
        checkFragments(fragments, cb, percentageOfLedgerFragmentToBeVerified);
    }

    private void checkFragments(Set<LedgerFragment> fragments,
                                GenericCallback<Set<LedgerFragment>> cb,
                                long percentageOfLedgerFragmentToBeVerified) {
        if (fragments.size() == 0) { // no fragments to verify
            cb.operationComplete(BKException.Code.OK, fragments);
            return;
        }

        // verify all the collected fragment replicas
        FullLedgerCallback allFragmentsCb = new FullLedgerCallback(fragments
                .size(), cb);
        for (LedgerFragment r : fragments) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking fragment {}", r);
            }
            try {
                verifyLedgerFragment(r, allFragmentsCb, percentageOfLedgerFragmentToBeVerified);
            } catch (InvalidFragmentException ife) {
                LOG.error("Invalid fragment found : {}", r);
                allFragmentsCb.operationComplete(
                        BKException.Code.IncorrectParameterException, r);
            } catch (BKException e) {
                LOG.error("BKException when checking fragment : {}", r, e);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException when checking fragment : {}", r, e);
            }
        }
    }

}
