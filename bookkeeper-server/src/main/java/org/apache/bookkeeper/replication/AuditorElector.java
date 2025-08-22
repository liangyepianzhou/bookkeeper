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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 使用 Apache ZooKeeper 进行 Auditor 选举。借助 ZooKeeper 作为协调服务，
 * 当 bookie 参与竞选 auditor 时，会在 ZooKeeper 上创建一个临时顺序文件（znode），作为它的投票。
 * 投票格式为 'V_序列号'。通过比较临时顺序 znode 的序号来完成选举，
 * 创建了最小序号 znode 的 bookie 将当选为 Auditor。
 * 其余 bookie 会根据顺序号监听自己的前驱 znode。
 */
@StatsDoc(
        name = AUDITOR_SCOPE,
        help = "Auditor 相关统计信息"
)
public class AuditorElector {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorElector.class);

    // 当前 bookie 的唯一标识（HostAddress:Port 格式）
    private final String bookieId;
    // server 配置
    private final ServerConfiguration conf;
    // 操作 BookKeeper 实例
    private final BookKeeper bkc;
    // 是否自己拥有 bkc 实例
    private final boolean ownBkc;
    // 线程池，只允许有一个线程（即选举线程）
    private final ExecutorService executor;
    // 管理 ledger 审计的 manager
    private final LedgerAuditorManager ledgerAuditorManager;

    // 审计者对象
    Auditor auditor;
    // 当前运行状态，true 为选举/审计正在运行
    private AtomicBoolean running = new AtomicBoolean(false);

    private final StatsLogger statsLogger;

    // 测试可见性构造方法，仅用于测试
    @VisibleForTesting
    public AuditorElector(final String bookieId, ServerConfiguration conf) throws UnavailableException {
        this(
                bookieId,
                conf,
                Auditor.createBookKeeperClientThrowUnavailableException(conf),
                true);
    }

    /**
     * 执行 auditor 选举的构造方法
     *
     * @param bookieId     bookie 的唯一标识（HostAddress:Port）
     * @param conf         服务器配置
     * @param bkc          BookKeeper 实例
     * @throws UnavailableException 初始化 elect 失败时抛出
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          boolean ownBkc) throws UnavailableException {
        this(bookieId, conf, bkc, NullStatsLogger.INSTANCE, ownBkc);
    }

    /**
     * 执行 auditor 选举的构造方法（完整参数版本）
     *
     * @param bookieId     bookie 的唯一标识（HostAddress:Port）
     * @param conf         配置项
     * @param bkc          BookKeeper 实例
     * @param statsLogger  统计日志记录器
     * @param ownBkc       是否拥有 bkc 实例
     * @throws UnavailableException 初始化 elect 失败时抛出
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          StatsLogger statsLogger,
                          boolean ownBkc) throws UnavailableException {
        this.bookieId = bookieId;
        this.conf = conf;
        this.bkc = bkc;
        this.ownBkc = ownBkc;
        this.statsLogger = statsLogger;
        try {
            // 创建 ledger 审计管理器，主要用于选举和通知
            this.ledgerAuditorManager = bkc.getLedgerManagerFactory().newLedgerAuditorManager();
        } catch (Exception e) {
            throw new UnavailableException("无法实例化 ledger auditor manager", e);
        }
        // 创建单线程池执行选举任务
        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "AuditorElector-" + bookieId);
            }
        });
    }

    // 启动选举，设置 running 状态并开始选举任务
    public Future<?> start() {
        running.set(true);
        return submitElectionTask();
    }

    /**
     * 执行清理操作，安全关闭 ledger auditor manager
     */
    private Future<?> submitShutdownTask() {
        return executor.submit(shutdownTask);
    }

    // 关闭操作任务，关闭 ledgerAuditorManager 等
    Runnable shutdownTask = new Runnable() {
        @Override
        public void run() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            try {
                ledgerAuditorManager.close();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("关闭 ledger auditor manager 时被中断", ie);
            } catch (Exception ke) {
                LOG.error("关闭 ledger auditor manager 时发生异常", ke);
            }
        }
    };

    /**
     * 使用 ZooKeeper 的临时顺序 znode 执行 auditor 选举。
     * 创建了最小序号 znode 的 bookie 成为 auditor。
     */
    @VisibleForTesting
    Future<?> submitElectionTask() {

        Runnable r = new Runnable() {
            @Override
            public void run() {
                if (!running.get()) {
                    return;
                }
                try {
                    // 尝试成为 auditor，监听 event（如会话丢失、投票删除等）
                    ledgerAuditorManager.tryToBecomeAuditor(bookieId, e -> handleAuditorEvent(e));

                    auditor = new Auditor(bookieId, conf, bkc, false, statsLogger);
                    auditor.start();
                } catch (InterruptedException e) {
                    LOG.error("执行 auditor 选举时被中断", e);
                    Thread.currentThread().interrupt();
                    submitShutdownTask();
                } catch (Exception e) {
                    LOG.error("执行 auditor 选举时发生异常", e);
                    submitShutdownTask();
                }
            }
        };
        try {
            // 提交到线程池执行
            return executor.submit(r);
        } catch (RejectedExecutionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("线程池已关闭");
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    // 处理审计事件（如会话丢失、投票被删除）
    private void handleAuditorEvent(LedgerAuditorManager.AuditorEvent e) {
        switch (e) {
            case SessionLost:
                LOG.error("失去 ZooKeeper 连接，准备关闭");
                submitShutdownTask();
                break;
            case VoteWasDeleted:
                submitElectionTask();
                break;
        }
    }

    // 在测试场景下获取当前 auditor 实例
    @VisibleForTesting
    Auditor getAuditor() {
        return auditor;
    }

    // 获取当前已当选的 auditor 的 BookieId
    public BookieId getCurrentAuditor() throws IOException, InterruptedException {
        return ledgerAuditorManager.getCurrentAuditor();
    }

    /**
     * 关闭 AuditorElector
     */
    public void shutdown() throws InterruptedException {
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }
            // 尝试优雅关闭 auditor manager 和线程池
            try {
                submitShutdownTask().get(10, TimeUnit.SECONDS);
                executor.shutdown();
            } catch (ExecutionException e) {
                LOG.warn("关闭 auditor manager 失败", e);
                executor.shutdownNow();
                shutdownTask.run();
            } catch (TimeoutException e) {
                LOG.warn("10秒内关闭 auditor manager 失败", e);
                executor.shutdownNow();
                shutdownTask.run();
            }
        }
        // 如果 auditor 已启动，关闭 auditor
        if (auditor != null) {
            auditor.shutdown();
            auditor = null;
        }
        // 如果拥有 bkc 实例，关闭 bookkeeper client
        if (ownBkc) {
            try {
                bkc.close();
            } catch (BKException e) {
                LOG.warn("关闭 bookkeeper client 失败", e);
            }
        }
    }

    /**
     * 如果当前 bookie 以 auditor 身份运行，返回 auditor 状态，否则返回 elector 状态
     *
     * @return 是否运行中
     */
    public boolean isRunning() {
        if (auditor != null) {
            return auditor.isRunning();
        }
        return running.get();
    }

    @Override
    public String toString() {
        return "AuditorElector for " + bookieId;
    }
}

