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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_HIGH_PRIORITY;
import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_NONE;
import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback.AddCallbackWithLatency;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp implements WriteCallback {
    private static final Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ByteBuf payload;
    ReferenceCounted toSend;
    AddCallbackWithLatency cb;
    Object ctx;
    long entryId;
    int entryLength;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;

    LedgerHandle lh;
    ClientContext clientCtx;
    boolean isRecoveryAdd = false;
    volatile long requestTimeNanos;
    long qwcLatency; // Quorum Write Completion Latency after response from quorum bookies.
    Set<BookieId> addEntrySuccessBookies;
    long writeDelayedStartTime; // min fault domains completion latency after response from ack quorum bookies

    long currentLedgerLength;
    int pendingWriteRequests;
    boolean callbackTriggered;
    boolean hasRun;
    EnumSet<WriteFlag> writeFlags;
    boolean allowFailFast = false;
    List<BookieId> ensemble;

    @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
    static PendingAddOp create(LedgerHandle lh, ClientContext clientCtx,
                               List<BookieId> ensemble,
                               ByteBuf payload, EnumSet<WriteFlag> writeFlags,
                               AddCallbackWithLatency cb, Object ctx) {
        PendingAddOp op = RECYCLER.get();
        op.lh = lh;
        op.clientCtx = clientCtx;
        op.isRecoveryAdd = false;
        op.cb = cb;
        op.ctx = ctx;
        op.entryId = LedgerHandle.INVALID_ENTRY_ID;
        op.currentLedgerLength = -1;
        op.payload = payload;
        op.entryLength = payload.readableBytes();

        op.completed = false;
        op.ensemble = ensemble;
        op.ackSet = lh.getDistributionSchedule().getAckSet();
        op.pendingWriteRequests = 0;
        op.callbackTriggered = false;
        op.hasRun = false;
        op.requestTimeNanos = Long.MAX_VALUE;
        op.allowFailFast = false;
        op.qwcLatency = 0;
        op.writeFlags = writeFlags;

        if (op.addEntrySuccessBookies == null) {
            op.addEntrySuccessBookies = new HashSet<>();
        } else {
            op.addEntrySuccessBookies.clear();
        }
        op.writeDelayedStartTime = -1;

        return op;
    }

    /**
     * Enable the recovery add flag for this operation.
     * @see LedgerHandle#asyncRecoveryAddEntry
     */
    PendingAddOp enableRecoveryAdd() {
        isRecoveryAdd = true;
        return this;
    }

    PendingAddOp allowFailFastOnUnwritableChannel() {
        allowFailFast = true;
        return this;
    }

    void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    void setLedgerLength(long ledgerLength) {
        this.currentLedgerLength = ledgerLength;
    }

    long getEntryId() {
        return this.entryId;
    }

    private void sendWriteRequest(List<BookieId> ensemble, int bookieIndex) {
        int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE;

        clientCtx.getBookieClient().addEntry(ensemble.get(bookieIndex),
                                             lh.ledgerId, lh.ledgerKey, entryId, toSend, this, bookieIndex,
                                             flags, allowFailFast, lh.writeFlags);
        ++pendingWriteRequests;
    }

    boolean maybeTimeout() {
        if (MathUtils.elapsedNanos(requestTimeNanos) >= clientCtx.getConf().addEntryQuorumTimeoutNanos) {
            timeoutQuorumWait();
            return true;
        }
        return false;
    }

    synchronized void timeoutQuorumWait() {
        if (completed) {
            return;
        }

        if (addEntrySuccessBookies.size() >= lh.getLedgerMetadata().getAckQuorumSize()) {
            // If ackQuorum number of bookies have acknowledged the write but still not complete, indicates
            // failures due to not having been written to enough fault domains. Increment corresponding
            // counter.
            clientCtx.getClientStats().getWriteTimedOutDueToNotEnoughFaultDomains().inc();
        }

        lh.handleUnrecoverableErrorDuringAdd(BKException.Code.AddEntryQuorumTimeoutException);
    }

    synchronized void unsetSuccessAndSendWriteRequest(List<BookieId> ensemble, int bookieIndex) {
        // update the ensemble
        this.ensemble = ensemble;

        if (toSend == null) {
            // this addOp hasn't yet had its mac computed. When the mac is
            // computed, its write requests will be sent, so no need to send it
            // now
            return;
        }
        // Suppose that unset doesn't happen on the write set of an entry. In this
        // case we don't need to resend the write request upon an ensemble change.
        // We do need to invoke #sendAddSuccessCallbacks() for such entries because
        // they may have already completed, but they are just waiting for the ensemble
        // to change.
        // E.g.
        // ensemble (A, B, C, D), entry k is written to (A, B, D). An ensemble change
        // happens to replace C with E. Entry k does not complete until C is
        // replaced with E successfully. When the ensemble change completes, it tries
        // to unset entry k. C however is not in k's write set, so no entry is written
        // again, and no one triggers #sendAddSuccessCallbacks. Consequently, k never
        // completes.
        //
        // We call sendAddSuccessCallback when unsetting t cover this case.
        if (!lh.distributionSchedule.hasEntry(entryId, bookieIndex)) {
            lh.sendAddSuccessCallbacks();
            return;
        }

        if (callbackTriggered) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                      + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        if (!ackSet.removeBookieAndCheck(bookieIndex)) {
            // unset completed if this results in loss of ack quorum
            completed = false;
        }

        sendWriteRequest(ensemble, bookieIndex);
    }

    /**
     * 发起（异步）添加条目操作。
     * 该方法在PendingAddOp准备好后被调用，负责构造数据包，
     * 检查Ledger状态，可能处理分布式写目标变化，并对每个副本Bookie推送写请求。
     */
    public synchronized void initiate() {
        hasRun = true; // 标记这个操作已触发，防止重复执行

        if (callbackTriggered) {
            // 如果回调已经被触发，说明这个请求因前序失败或者被取消，则无需再执行
            // 一般是由于pending队列里前面的操作失败，导致本操作也提前失败了
            maybeRecycle(); // 对象复用，释放资源
            return;
        }

        this.requestTimeNanos = MathUtils.nowInNano(); // 记录本次写入请求触发的时间戳

        checkNotNull(lh); // ledger句柄非空校验，防止空指针异常
        checkNotNull(lh.macManager); // 消息鉴权管理器非空校验

        // 决定写入标志位（是否为恢复性写，以及是否设置高优先级）
        int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE;

        // 用于计算消息摘要（MAC），同时将要写入的数据（payload）封装成待发送的数据包（toSend）
        // 注意：computeDigestAndPackageForSending会接管payload的引用计数管理（ByteBuf），所以payload置为null防止重复释放
        this.toSend = lh.macManager.computeDigestAndPackageForSending(
                entryId,
                lh.lastAddConfirmed,
                currentLedgerLength,
                payload,
                lh.ledgerKey,
                flags
        );
        payload = null; // 标志payload资源交由macManager管理

        // 处理“延迟写错误”场景，Bookie集群可能有坏节点，需要重排target ensemble
        lh.maybeHandleDelayedWriteBookieFailure();

        // 遍历分布式写副本集（quorum）并发起真正的写请求
        for (int i = 0; i < lh.distributionSchedule.getWriteQuorumSize(); i++) {
            // 每个Bookie的下标按分布策略取出，然后依次发送写请求
            sendWriteRequest(
                    ensemble, // 当前写副本集
                    lh.distributionSchedule.getWriteSetBookieIndex(entryId, i) // 副本集内bookie的下标
            );
        }
    }


    @Override
    public synchronized void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
        int bookieIndex = (Integer) ctx; // 获得当前写入请求对应的bookie索引
        --pendingWriteRequests; // 当前待写请求数减一

        // 如果ensemble（副本列表）已经改变，则该节点失败无关紧要
        if (!ensemble.get(bookieIndex).equals(addr)) {
            // 副本组已经变化，此节点失败无关紧要
            if (LOG.isDebugEnabled()) {
                LOG.debug("写入失败: " + ledgerId + ", " + entryId + ". 但已自动修复。");
            }
            return;
        }

        // 必须记录所有ack，即使已完成（因为副本组变化可能撤销完成状态）
        boolean ackQuorum = false;
        if (BKException.Code.OK == rc) { // 如果返回码OK，说明本次写入成功
            ackQuorum = ackSet.completeBookieAndCheck(bookieIndex); // 标记bookie响应，并检查是否已满足Quorum
            addEntrySuccessBookies.add(ensemble.get(bookieIndex)); // 记录成功响应的bookie
        }

        if (completed) { // 如果操作已完成，但收到异常应答
            if (rc != BKException.Code.OK) {
                // AQ已满足后收到错误，说明创建时已处于欠副本状态，统计计数
                clientCtx.getClientStats().getAddOpUrCounter().inc();
                // 如果没有禁用ensemble切换，并且不是延迟切换副本组，则通知处理写失败
                if (!clientCtx.getConf().disableEnsembleChangeFeature.isAvailable()
                        && !clientCtx.getConf().delayEnsembleChange) {
                    lh.notifyWriteFailed(bookieIndex, addr);
                }
            }
            // 虽然add操作已完成，但由于某些处理未重置completed标志，导致没有回调成功，需要补充回调
            //
            // 场景举例：
            // 1）x+k条写入失败，handleBookieFailure增加blockAddCompletions阻止完成
            // 2）x写入收到所有应答，设置completed=true，但未回调成功，因为blockAddCompletions阻止
            // 3）副本组切换完成，lh unset success从x到x+k，但操作未违反ackSet约束，completed仍为true
            // 4）新bookie再次重试写入完成，发现pending op已完成，故需要再次回调
            //
            sendAddSuccessCallbacks(); // 执行成功回调
            // 我已经完成，忽略后续应答，否则可能出错
            maybeRecycle();
            return;
        }

        // 针对不同的返回码做异常判断和处理
        switch (rc) {
            case BKException.Code.OK:
                // 写入成功，继续
                break;
            case BKException.Code.ClientClosedException:
                // 客户端已关闭，所有pending写操作都失败
                lh.errorOutPendingAdds(rc);
                return;
            case BKException.Code.IllegalOpException:
                // 非法操作，比如协议不支持的特性
                lh.handleUnrecoverableErrorDuringAdd(rc);
                return;
            case BKException.Code.LedgerFencedException:
                // ledger被Fenced（锁定），不可写
                LOG.warn("写入Fenced异常: L{} E{} 于 {}",
                        ledgerId, entryId, addr);
                lh.handleUnrecoverableErrorDuringAdd(rc);
                return;
            case BKException.Code.UnauthorizedAccessException:
                // 没有写入权限
                LOG.warn("写入权限异常: L{} E{} 于 {}",
                        ledgerId, entryId, addr);
                lh.handleUnrecoverableErrorDuringAdd(rc);
                return;
            default:
                // 其它异常处理，是否延迟切换副本组
                if (clientCtx.getConf().delayEnsembleChange) {
                    if (ackSet.failBookieAndCheck(bookieIndex, addr)
                            || rc == BKException.Code.WriteOnReadOnlyBookieException) {
                        // bookie挂了或变只读，获取所有已失败bookie索引与地址
                        Map<Integer, BookieId> failedBookies = ackSet.getFailedBookies();
                        LOG.warn("写入({},{})失败到bookies {}, 处理异常。",
                                ledgerId, entryId, failedBookies);
                        // 无法满足Quorum，触发ensemble切换
                        lh.handleBookieFailure(failedBookies);
                    } else if (LOG.isDebugEnabled()) {
                        // 未破坏Quorum，延迟副本组切换
                        LOG.debug("写入({},{})失败于bookie({},{})，未破坏Quorum，延迟切换: {}",
                                ledgerId, entryId, bookieIndex, addr, BKException.getMessage(rc));
                    }
                } else {
                    LOG.warn("写入({},{})失败于bookie({},{})：{}",
                            ledgerId, entryId, bookieIndex, addr, BKException.getMessage(rc));
                    lh.handleBookieFailure(ImmutableMap.of(bookieIndex, addr)); // 非延迟，直接处理副本组切换
                }
                return;
        }

        // 满足ackQuorum且尚未完成时，做所有约束检查
        if (ackQuorum && !completed) {
            // 检查写入应答的bookie是否分布在足够多的Fault Domain中
            if (clientCtx.getConf().enforceMinNumFaultDomainsForWrite
                    && !(clientCtx.getPlacementPolicy()
                    .areAckedBookiesAdheringToPlacementPolicy(addEntrySuccessBookies,
                            lh.getLedgerMetadata().getWriteQuorumSize(),
                            lh.getLedgerMetadata().getAckQuorumSize()))) {
                LOG.warn("entry ID {} 写入成功被延迟，因为Fault Domain数不足", entryId);
                // 统计因为故障域不足造成的延迟
                clientCtx.getClientStats().getWriteDelayedDueToNotEnoughFaultDomains().inc();

                // 仅第一次写入延迟时记录开始时间
                if (writeDelayedStartTime == -1) {
                    writeDelayedStartTime = MathUtils.nowInNano();
                }
            } else {
                completed = true; // 操作完成
                this.qwcLatency = MathUtils.elapsedNanos(requestTimeNanos); // 记录延迟

                if (writeDelayedStartTime != -1) {
                    clientCtx.getClientStats()
                            .getWriteDelayedDueToNotEnoughFaultDomainsLatency()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(writeDelayedStartTime),
                                    TimeUnit.NANOSECONDS);
                }
                sendAddSuccessCallbacks(); // 执行成功回调
            }
        }
    }


    void sendAddSuccessCallbacks() {
        lh.sendAddSuccessCallbacks();
    }

    synchronized void submitCallback(final int rc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Submit callback (lid:{}, eid: {}). rc:{}", lh.getId(), entryId, rc);
        }

        long latencyNanos = MathUtils.elapsedNanos(requestTimeNanos);
        if (rc != BKException.Code.OK) {
            clientCtx.getClientStats().getAddOpLogger().registerFailedEvent(latencyNanos, TimeUnit.NANOSECONDS);
            LOG.error("Write of ledger entry to quorum failed: L{} E{}",
                      lh.getId(), entryId);
        } else {
            clientCtx.getClientStats().getAddOpLogger().registerSuccessfulEvent(latencyNanos, TimeUnit.NANOSECONDS);
        }
        cb.addCompleteWithLatency(rc, lh, entryId, qwcLatency, ctx);
        callbackTriggered = true;

        maybeRecycle();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PendingAddOp(lid:").append(lh.ledgerId)
          .append(", eid:").append(entryId).append(", completed:")
          .append(completed).append(")");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return (int) entryId;
    }

    @Override
    public boolean equals(Object o) {
       if (o instanceof PendingAddOp) {
           return (this.entryId == ((PendingAddOp) o).entryId);
       }
       return (this == o);
    }

    private final Handle<PendingAddOp> recyclerHandle;
    private static final Recycler<PendingAddOp> RECYCLER = new Recycler<PendingAddOp>() {
        @Override
        protected PendingAddOp newObject(Recycler.Handle<PendingAddOp> handle) {
            return new PendingAddOp(handle);
        }
    };

    private PendingAddOp(Handle<PendingAddOp> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }


    private synchronized void maybeRecycle() {
        /**
         * We have opportunity to recycle two objects here.
         * PendingAddOp#toSend and LedgerHandle#pendingAddOp
         *
         * A. LedgerHandle#pendingAddOp: This can be released after all 3 conditions are met.
         *    - After the callback is run
         *    - After safeRun finished by the executor
         *    - Write responses are returned from all bookies
         *      as BookieClient Holds a reference from the point the addEntry requests are sent.
         *
         * B. ByteBuf can be released after 2 of the conditions are met.
         *    - After the callback is run as we will not retry the write after callback
         *    - After safeRun finished by the executor
         * BookieClient takes and releases on this buffer immediately after sending the data.
         *
         * The object can only be recycled after the above conditions are met
         * otherwise we could end up recycling twice and all
         * joy that goes along with that.
         */
        if (hasRun && callbackTriggered) {
            ReferenceCountUtil.release(toSend);
            toSend = null;
        }
        // only recycle a pending add op after it has been run.
        if (hasRun && toSend == null && pendingWriteRequests == 0) {
            recyclePendAddOpObject();
        }
    }

    public synchronized void recyclePendAddOpObject() {
        entryId = LedgerHandle.INVALID_ENTRY_ID;
        currentLedgerLength = -1;
        if (payload != null) {
            ReferenceCountUtil.release(payload);
            payload = null;
        }
        cb = null;
        ctx = null;
        ensemble = null;
        ackSet.recycle();
        ackSet = null;
        lh = null;
        clientCtx = null;
        isRecoveryAdd = false;
        completed = false;
        pendingWriteRequests = 0;
        callbackTriggered = false;
        hasRun = false;
        allowFailFast = false;
        writeFlags = null;
        addEntrySuccessBookies.clear();
        writeDelayedStartTime = -1;

        recyclerHandle.recycle(this);
    }
}
