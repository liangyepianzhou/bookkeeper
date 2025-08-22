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
package org.apache.bookkeeper.meta;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.proto.DataFormats.AuditorVoteFormat;
import static org.apache.bookkeeper.replication.ReplicationStats.ELECTION_ATTEMPTS;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * ZK based implementation of LedgerAuditorManager.
 */
@Slf4j
public class ZkLedgerAuditorManager implements LedgerAuditorManager {

    private final ZooKeeper zkc;
    private final ServerConfiguration conf;
    private final String basePath;
    private final String electionPath;

    private String myVote;

    private static final String ELECTION_ZNODE = "auditorelection";

    // Represents the index of the auditor node
    private static final int AUDITOR_INDEX = 0;
    // Represents vote prefix
    private static final String VOTE_PREFIX = "V_";
    // Represents path Separator
    private static final String PATH_SEPARATOR = "/";

    private volatile Consumer<AuditorEvent> listener;
    private volatile boolean isClosed = false;

    // Expose Stats
    @StatsDoc(
            name = ELECTION_ATTEMPTS,
            help = "The number of auditor election attempts"
    )
    private final Counter electionAttempts;

    public ZkLedgerAuditorManager(ZooKeeper zkc, ServerConfiguration conf, StatsLogger statsLogger) {
        this.zkc = zkc;
        this.conf = conf;

        this.basePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        this.electionPath = basePath + '/' + ELECTION_ZNODE;
        this.electionAttempts = statsLogger.getCounter(ELECTION_ATTEMPTS);
    }

    @Override
    public void tryToBecomeAuditor(String bookieId, Consumer<AuditorEvent> listener)
            throws IOException, InterruptedException {
        // 设置事件监听器，用于通知外部处理审计相关事件
        this.listener = listener;
        // 创建选举路径（ZooKeeper临时节点的父节点），用于存储投票信息
        createElectorPath();

        try {
            // 主循环，在没有关闭的情况下不断尝试当选为Auditor
            while (!isClosed) {
                // 创建本节点的投票（临时顺序znode）
                createMyVote(bookieId);

                // 获取所有参与选举的子节点列表（按照zk顺序编号）
                List<String> children = zkc.getChildren(getVotePath(""), false);
                if (0 >= children.size()) {
                    // 至少需要一个bookie参与选举，否则抛异常
                    throw new IllegalArgumentException(
                            "必须至少有一个 bookie server 才能选举Auditor！");
                }

                // 按照节点序号升序排序
                Collections.sort(children, new ElectionComparator());
                // 获取自己节点的名称（去除路径，只要节点名，如V_0000000001）
                String voteNode = StringUtils.substringAfterLast(myVote, PATH_SEPARATOR);

                // 排在最前的节点为Auditor（编号最小）
                if (children.get(AUDITOR_INDEX).equals(voteNode)) {
                    // 已经被选举为Auditor
                    // 在选举路径记录Auditor的bookieId（仅用于调试）
                    AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                            .setBookieId(bookieId);

                    zkc.setData(getVotePath(""),
                            builder.build().toString().getBytes(UTF_8), -1);
                    return;
                } else {
                    // 如果没当选为Auditor，监听自己的前一个节点（前驱节点）
                    int myIndex = children.indexOf(voteNode);
                    if (myIndex < 0) {
                        // 如果本节点被删除（极端情况），报错
                        throw new IllegalArgumentException("本节点投票已消失");
                    }

                    int prevNodeIndex = myIndex - 1;

                    // 用CountDownLatch等待事件（前一个节点被删除）
                    CountDownLatch latch = new CountDownLatch(1);

                    // 检查前驱节点是否存在，并注册监听
                    if (null == zkc.exists(getVotePath(PATH_SEPARATOR)
                            + children.get(prevNodeIndex), event -> latch.countDown())) {
                        // 如果前驱节点不存在，重试选举流程
                        continue;
                    }

                    // 等待前驱节点被删除（即有bookie下线），再次尝试选举
                    latch.await();
                }

                // 记录选举尝试次数
                electionAttempts.inc();
            }
        } catch (KeeperException e) {
            // ZooKeeper抛出的异常包装为IO异常
            throw new IOException(e);
        }
    }

    @Override
    public BookieId getCurrentAuditor() throws IOException, InterruptedException {
        String electionRoot = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE + '/' + ELECTION_ZNODE;

        try {
            List<String> children = zkc.getChildren(electionRoot, false);
            Collections.sort(children, new ElectionComparator());
            if (children.size() < 1) {
                return null;
            }
            String ledger = electionRoot + "/" + children.get(AUDITOR_INDEX);
            byte[] data = zkc.getData(ledger, false, null);

            AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder();
            TextFormat.merge(new String(data, UTF_8), builder);
            AuditorVoteFormat v = builder.build();
            return BookieId.parse(v.getBookieId());
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down AuditorElector");
        isClosed = true;
        if (myVote != null) {
            try {
                zkc.delete(myVote, -1);
            } catch (KeeperException.NoNodeException nne) {
                // Ok
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("InterruptedException while deleting myVote: " + myVote,
                        ie);
            } catch (KeeperException ke) {
                log.error("Exception while deleting myVote:" + myVote, ke);
            }
        }
    }

    /**
     * 在 ZooKeeper 上为当前 bookie 创建投票节点（临时顺序节点）。
     * 该节点用于参与 Auditor 选举，节点内容记录当前 bookie 的 ID。
     *
     * @param bookieId 当前 bookie 的唯一标识符
     * @throws IOException ZooKeeper 操作异常
     * @throws InterruptedException 线程中断异常
     */
    private void createMyVote(String bookieId) throws IOException, InterruptedException {
        // 获取配置文件中的 ACL 权限设置
        List<ACL> zkAcls = ZkUtils.getACLs(conf);

        // 构建节点数据（包含 bookieId），用于标识该投票节点归属哪个 bookie
        AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                .setBookieId(bookieId);

        try {
            // 首次投票（myVote 为 null），或者之前节点已经不存在（被删除、长时间未连接等），需要重新创建投票节点
            if (null == myVote || null == zkc.exists(myVote, false)) {
                // 在选举路径下创建临时顺序节点（EPHEMERAL_SEQUENTIAL），节点名称前缀为 VOTE_PREFIX
                // 节点内容为当前 bookieId，权限为zkAcls
                myVote = zkc.create(getVotePath(PATH_SEPARATOR + VOTE_PREFIX),
                        builder.build().toString().getBytes(UTF_8), zkAcls,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                // 保存节点路径到 myVote，后续用于选举及监听
            }
        } catch (KeeperException e) {
            // 若 ZooKeeper 操作异常，抛出 IO 异常（如连接丢失、权限不足等）
            throw new IOException(e);
        }
    }

    private void createElectorPath() throws IOException {
        try {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            if (zkc.exists(basePath, false) == null) {
                try {
                    zkc.create(basePath, new byte[0], zkAcls,
                            CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // do nothing, someone else could have created it
                }
            }
            if (zkc.exists(getVotePath(""), false) == null) {
                try {
                    zkc.create(getVotePath(""), new byte[0],
                            zkAcls, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // do nothing, someone else could have created it
                }
            }
        } catch (KeeperException ke) {
            throw new IOException("Failed to initialize Auditor Elector", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to initialize Auditor Elector", ie);
        }
    }

    private String getVotePath(String vote) {
        return electionPath + vote;
    }

    private void handleZkWatch(WatchedEvent event) {
        if (isClosed) {
            return;
        }

        if (event.getState() == Watcher.Event.KeeperState.Expired) {
            log.error("Lost ZK connection, shutting down");

            listener.accept(AuditorEvent.SessionLost);
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            listener.accept(AuditorEvent.VoteWasDeleted);
        }
    }

    /**
     * Compare the votes in the ascending order of the sequence number. Vote
     * format is 'V_sequencenumber', comparator will do sorting based on the
     * numeric sequence value.
     */
    private static class ElectionComparator
            implements Comparator<String>, Serializable {
        /**
         * Return -1 if the first vote is less than second. Return 1 if the
         * first vote is greater than second. Return 0 if the votes are equal.
         */
        @Override
        public int compare(String vote1, String vote2) {
            long voteSeqId1 = getVoteSequenceId(vote1);
            long voteSeqId2 = getVoteSequenceId(vote2);
            int result = voteSeqId1 < voteSeqId2 ? -1
                    : (voteSeqId1 > voteSeqId2 ? 1 : 0);
            return result;
        }

        private long getVoteSequenceId(String vote) {
            String voteId = StringUtils.substringAfter(vote, VOTE_PREFIX);
            return Long.parseLong(voteId);
        }
    }

}
