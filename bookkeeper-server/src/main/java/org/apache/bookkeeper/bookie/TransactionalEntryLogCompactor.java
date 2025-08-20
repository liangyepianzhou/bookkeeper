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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.CompactionEntryLog;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for compaction. Compaction is done in several transactional phases.
 * Phase 1: Scan old entry log and compact entries to a new .compacting log file.
 * Phase 2: Flush .compacting log to disk and it becomes .compacted log file when this completes.
 * Phase 3: Flush ledger cache and .compacted file becomes .log file when this completes. Remove old
 * entry log file afterwards.
 */
public class TransactionalEntryLogCompactor extends AbstractLogCompactor {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalEntryLogCompactor.class);

    final EntryLogger entryLogger;
    final CompactableLedgerStorage ledgerStorage;
    final List<EntryLocation> offsets = new ArrayList<>();

    // compaction log file suffix
    public static final String COMPACTING_SUFFIX = ".log.compacting";
    // flushed compaction log file suffix
    public static final String COMPACTED_SUFFIX = ".compacted";

    public TransactionalEntryLogCompactor(
            ServerConfiguration conf,
            EntryLogger entryLogger,
            CompactableLedgerStorage ledgerStorage,
            LogRemovalListener logRemover) {
        super(conf, logRemover);
        this.entryLogger = entryLogger;
        this.ledgerStorage = ledgerStorage;
    }

    /**
     * Delete all previously incomplete compacting logs and recover the index for compacted logs.
     */
    @Override
    public void cleanUpAndRecover() {
        // clean up compacting logs and recover index for already compacted logs
        for (CompactionEntryLog log : entryLogger.incompleteCompactionLogs()) {
            LOG.info("Found compacted log file {} has partially flushed index, recovering index.", log);
            CompactionPhase updateIndex = new UpdateIndexPhase(log, true);
            updateIndex.run();
        }
    }

    /**
     * 对指定 entry log 文件执行数据压缩（compaction）流程。
     *
     * 压缩的目的是将 entry log 文件中 remaining size percentage（还在使用的 ledger 数据）迁移到新的 compact log，
     * 达到有效清理/复用空间的目的。
     *
     * 主要流程分为三个阶段，任何一个阶段失败，均中止当前 entry log 的压缩：
     * 1. ScanEntryLogPhase   : 遍历原 entry log，找到所有还在使用的 ledger 数据（remaining size percentage的部分）并迁移到新的 compaction log。
     * 2. FlushCompactionLogPhase : 将 compaction log 中的数据（迁移的 ledger 记录）落盘，确保新日志文件稳定可靠。
     * 3. UpdateIndexPhase       : 更新索引，将 ledger 映射更新为新的 log 文件 id，保证系统后续访问 ledger 能定位到新日志文件。
     *
     * 日志打印会记录每个阶段的起止、异常等信息，便于故障排查与运维。
     *
     * @param metadata EntryLogMetadata，指定要压缩的 entry log 文件元数据
     * @return true 表示压缩成功；false 表示压缩过程中有任意阶段失败
     */
    @Override
    public boolean compact(EntryLogMetadata metadata) {
        if (metadata != null) {
            LOG.info("Compacting entry log {} with usage {}.",
                    metadata.getEntryLogId(), metadata.getUsage()); // usage 表示剩余还在用的数据比例 remaining size percentage

            CompactionEntryLog compactionLog;
            try {
                // 创建用于压缩数据迁移的新日志文件对象
                compactionLog = entryLogger.newCompactionLog(metadata.getEntryLogId());
            } catch (IOException ioe) {
                LOG.error("Exception creating new compaction entry log", ioe);
                return false;
            }

            // 第一阶段：扫描原 entry log，迁移所有剩余有效 ledger 数据
            CompactionPhase scanEntryLog = new ScanEntryLogPhase(metadata, compactionLog);
            if (!scanEntryLog.run()) {
                LOG.info("Compaction for entry log {} end in ScanEntryLogPhase.", metadata.getEntryLogId());
                return false;
            }

            // 第二阶段：落盘新的 compaction log，确保数据可靠持久化
            CompactionPhase flushCompactionLog = new FlushCompactionLogPhase(compactionLog);
            if (!flushCompactionLog.run()) {
                LOG.info("Compaction for entry log {} end in FlushCompactionLogPhase.", metadata.getEntryLogId());
                return false;
            }

            // 第三阶段：更新 ledger 到 entry log 的索引指向，便于后续读写定位
            CompactionPhase updateIndex = new UpdateIndexPhase(compactionLog);
            if (!updateIndex.run()) {
                LOG.info("Compaction for entry log {} end in UpdateIndexPhase.", metadata.getEntryLogId());
                return false;
            }

            LOG.info("Compacted entry log : {}.", metadata.getEntryLogId()); // 全流程成功
            return true;
        }
        return false;
    }


    /**
     * An abstract class that would be extended to be the actual transactional phases for compaction.
     */
    abstract static class CompactionPhase {
        private String phaseName = "";

        CompactionPhase(String phaseName) {
            this.phaseName = phaseName;
        }

        boolean run() {
            try {
                start();
                return complete();
            } catch (IOException e) {
                LOG.error("Encounter exception in compaction phase {}. Abort current compaction.", phaseName, e);
                abort();
            }
            return false;
        }

        abstract void start() throws IOException;

        abstract boolean complete() throws IOException;

        abstract void abort();

    }

    /**
     * Assume we're compacting entry log 1 to entry log 3.
     * The first phase is to scan entries in 1.log and copy them to compaction log file "3.log.compacting".
     * We'll try to allocate a new compaction log before scanning to make sure we have a log file to write.
     * If after scanning, there's no data written, it means there's no valid entries to be compacted,
     * so we can remove 1.log directly, clear the offsets and end the compaction.
     * Otherwise, we should move on to the next phase.
     *
     * <p>If anything failed in this phase, we should delete the compaction log and clean the offsets.
     */
    class ScanEntryLogPhase extends CompactionPhase {
        private final EntryLogMetadata metadata;
        private final CompactionEntryLog compactionLog;

        ScanEntryLogPhase(EntryLogMetadata metadata, CompactionEntryLog compactionLog) {
            super("ScanEntryLogPhase");
            this.metadata = metadata;
            this.compactionLog = compactionLog;
        }

        @Override
        void start() throws IOException {
            // scan entry log into compaction log and offset list
            entryLogger.scanEntryLog(metadata.getEntryLogId(), new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return metadata.containsLedger(ledgerId);
                }

                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    throttler.acquire(entry.readableBytes());
                    synchronized (TransactionalEntryLogCompactor.this) {
                        long lid = entry.getLong(entry.readerIndex());
                        long entryId = entry.getLong(entry.readerIndex() + 8);
                        if (lid != ledgerId || entryId < -1) {
                            LOG.warn("Scanning expected ledgerId {}, but found invalid entry "
                                    + "with ledgerId {} entryId {} at offset {}",
                                    ledgerId, lid, entryId, offset);
                            throw new IOException("Invalid entry found @ offset " + offset);
                        }
                        long newOffset = compactionLog.addEntry(ledgerId, entry);
                        offsets.add(new EntryLocation(ledgerId, entryId, newOffset));

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Compact add entry : lid = {}, eid = {}, offset = {}",
                                    ledgerId, entryId, newOffset);
                        }
                    }
                }
            });
        }

        @Override
        boolean complete() {
            if (offsets.isEmpty()) {
                // no valid entries is compacted, delete entry log file
                LOG.info("No valid entry is found in entry log after scan, removing entry log now.");
                logRemovalListener.removeEntryLog(metadata.getEntryLogId());
                compactionLog.abort();
                compactingLogWriteDone();
                return false;
            }
            return true;
        }

        @Override
        void abort() {
            offsets.clear();
            // since we haven't flushed yet, we only need to delete the unflushed compaction file.
            compactionLog.abort();
            compactingLogWriteDone();
        }
    }

    private void compactingLogWriteDone() {
        if (entryLogger instanceof DefaultEntryLogger) {
            ((DefaultEntryLogger) entryLogger).clearCompactingLogId();
        }
    }

    /**
     * Assume we're compacting log 1 to log 3.
     * This phase is to flush the compaction log.
     * When this phase starts, there should be a compaction log file like "3.log.compacting"
     * When compaction log is flushed, in order to indicate this phase is completed,
     * a hardlink file "3.log.1.compacted" should be created, and "3.log.compacting" should be deleted.
     */
    class FlushCompactionLogPhase extends CompactionPhase {
        final CompactionEntryLog compactionLog;

        FlushCompactionLogPhase(CompactionEntryLog compactionLog) {
            super("FlushCompactionLogPhase");
            this.compactionLog = compactionLog;
        }

        @Override
        void start() throws IOException {
            // flush the current compaction log.
            compactionLog.flush();
        }

        @Override
        boolean complete() throws IOException {
            try {
                compactionLog.markCompacted();
                return true;
            } catch (IOException ioe) {
                LOG.warn("Error marking compaction as done", ioe);
                return false;
            } finally {
                compactingLogWriteDone();
            }
        }

        @Override
        void abort() {
            offsets.clear();
            // remove compaction log file and its hardlink
            compactionLog.abort();
            compactingLogWriteDone();
        }
    }

    /**
     * Assume we're compacting log 1 to log 3.
     * This phase is to update the entry locations and flush the index.
     * When the phase start, there should be a compacted file like "3.log.1.compacted",
     * where 3 is the new compaction logId and 1 is the old entry logId.
     * After the index the flushed successfully, a hardlink "3.log" file should be created,
     * and 3.log.1.compacted file should be deleted to indicate the phase is succeed.
     *
     * <p>This phase can also used to recover partially flushed index when we pass isInRecovery=true
     */
    class UpdateIndexPhase extends CompactionPhase {
        final CompactionEntryLog compactionLog;
        private final boolean isInRecovery;

        public UpdateIndexPhase(CompactionEntryLog compactionLog) {
            this(compactionLog, false);
        }

        public UpdateIndexPhase(CompactionEntryLog compactionLog, boolean isInRecovery) {
            super("UpdateIndexPhase");
            this.compactionLog = compactionLog;
            this.isInRecovery = isInRecovery;
        }

        @Override
        void start() throws IOException {
            compactionLog.makeAvailable();
            if (isInRecovery) {
                recoverEntryLocations(compactionLog);
            }
            if (!offsets.isEmpty()) {
                // update entry locations and flush index
                ledgerStorage.updateEntriesLocations(offsets);
                ledgerStorage.flushEntriesLocationsIndex();
            }
        }

        @Override
        boolean complete() {
            // When index is flushed, and entry log is removed,
            // delete the ".compacted" file to indicate this phase is completed.
            offsets.clear();
            compactionLog.finalizeAndCleanup();
            logRemovalListener.removeEntryLog(compactionLog.getSrcLogId());
            return true;
        }

        @Override
        void abort() {
            offsets.clear();
        }

        /**
         * Scan entry log to recover entry locations.
         */
        private void recoverEntryLocations(CompactionEntryLog compactionLog) throws IOException {
            compactionLog.scan(new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return true;
                }

                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    long lid = entry.getLong(entry.readerIndex());
                    long entryId = entry.getLong(entry.readerIndex() + 8);
                    if (lid != ledgerId || entryId < -1) {
                        LOG.warn("Scanning expected ledgerId {}, but found invalid entry "
                                + "with ledgerId {} entryId {} at offset {}",
                                ledgerId, lid, entryId, offset);
                        throw new IOException("Invalid entry found @ offset " + offset);
                    }
                    long location = (compactionLog.getDstLogId() << 32L) | (offset + 4);
                    offsets.add(new EntryLocation(lid, entryId, location));
                }
            });
            LOG.info("Recovered {} entry locations from compacted log {}", offsets.size(), compactionLog.getDstLogId());
        }
    }
}
