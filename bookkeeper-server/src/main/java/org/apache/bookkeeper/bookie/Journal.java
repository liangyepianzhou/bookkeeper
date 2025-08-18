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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.stats.JournalStats;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.common.collections.BatchedBlockingQueue;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.MemoryLimitController;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieRequestHandler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide journal related management.
 */
public class Journal implements CheckpointSource {

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);

    private static final RecyclableArrayList.Recycler<QueueEntry> entryListRecycler =
        new RecyclableArrayList.Recycler<QueueEntry>();

    private BookieCriticalThread thread;

    /**
     * Filter to pickup journals.
     */
    public interface JournalIdFilter {
        boolean accept(long journalId);
    }

    /**
     * For testability.
     */
    @FunctionalInterface
    public interface BufferedChannelBuilder {
        BufferedChannelBuilder DEFAULT_BCBUILDER = (FileChannel fc,
                int capacity) -> new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);

        BufferedChannel create(FileChannel fc, int capacity) throws IOException;
    }



    /**
     * List all journal ids by a specified journal id filer.
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    public static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File[] logFiles = journalDir.listFiles();
        if (logFiles == null || logFiles.length == 0) {
            return Collections.emptyList();
        }
        List<Long> logs = new ArrayList<Long>();
        for (File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * A wrapper over log mark to provide a checkpoint for users of journal
     * to do checkpointing.
     */
    private static class LogMarkCheckpoint implements Checkpoint {
        final LastLogMark mark;

        public LogMarkCheckpoint(LastLogMark checkpoint) {
            this.mark = checkpoint;
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (o == Checkpoint.MAX) {
                return -1;
            } else if (o == Checkpoint.MIN) {
                return 1;
            }
            return mark.getCurMark().compare(((LogMarkCheckpoint) o).mark.getCurMark());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LogMarkCheckpoint)) {
                return false;
            }
            return 0 == compareTo((LogMarkCheckpoint) o);
        }

        @Override
        public int hashCode() {
            return mark.hashCode();
        }

        @Override
        public String toString() {
            return mark.toString();
        }
    }

    /**
     * Last Log Mark.
     */
    public class LastLogMark {
        private final LogMark curMark;

        LastLogMark(long logId, long logPosition) {
            this.curMark = new LogMark(logId, logPosition);
        }

        void setCurLogMark(long logId, long logPosition) {
            curMark.setLogMark(logId, logPosition);
        }

        LastLogMark markLog() {
            return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
        }

        public LogMark getCurMark() {
            return curMark;
        }

        void rollLog(LastLogMark lastMark) throws NoWritableLedgerDirException {
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            lastMark.getCurMark().writeLogMark(bb);

            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : {}", lastMark.getCurMark());
            }

            List<File> writableLedgerDirs = ledgerDirsManager
                    .getWritableLedgerDirsForNewLog();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, lastMarkFileName);
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                    fos = null;
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                } finally {
                    // if stream already closed in try block successfully,
                    // stream might have nullified, in such case below
                    // call will simply returns
                    IOUtils.close(LOG, fos);
                }
            }
        }

        /**
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        public void readLog() {
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            LogMark mark = new LogMark();
            for (File dir: ledgerDirsManager.getAllLedgerDirs()) {
                File file = new File(dir, lastMarkFileName);
                try {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        int bytesRead = fis.read(buff);
                        if (bytesRead != 16) {
                            throw new IOException("Couldn't read enough bytes from lastMark."
                                                  + " Wanted " + 16 + ", got " + bytesRead);
                        }
                    }
                    bb.clear();
                    mark.readLogMark(bb);
                    if (curMark.compare(mark) < 0) {
                        curMark.setLogMark(mark.getLogFileId(), mark.getLogFileOffset());
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this "
                            + "bookie");
                }
            }
        }

        @Override
        public String toString() {
            return curMark.toString();
        }
    }

    /**
     * Filter to return list of journals for rolling.
     */
    private static class JournalRollingFilter implements JournalIdFilter {

        final LastLogMark lastMark;

        JournalRollingFilter(LastLogMark lastMark) {
            this.lastMark = lastMark;
        }

        @Override
        public boolean accept(long journalId) {
            return journalId < lastMark.getCurMark().getLogFileId();
        }
    }

    /**
     * Scanner used to scan a journal.
     */
    public interface JournalScanner {
        /**
         * Process a journal entry.
         *
         * @param journalVersion Journal Version
         * @param offset File offset of the journal entry
         * @param entry Journal Entry
         * @throws IOException
         */
        void process(int journalVersion, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Journal Entry to Record.
     */
    static class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;
        boolean ackBeforeSync;

        OpStatsLogger journalAddEntryStats;
        Counter callbackTime;

        static QueueEntry create(ByteBuf entry, boolean ackBeforeSync, long ledgerId, long entryId,
                WriteCallback cb, Object ctx, long enqueueTime, OpStatsLogger journalAddEntryStats,
                Counter callbackTime) {
            QueueEntry qe = RECYCLER.get();
            qe.entry = entry;
            qe.ackBeforeSync = ackBeforeSync;
            qe.cb = cb;
            qe.ctx = ctx;
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.enqueueTime = enqueueTime;
            qe.journalAddEntryStats = journalAddEntryStats;
            qe.callbackTime = callbackTime;
            return qe;
        }

        @Override
        public void run() {
            long startTime = System.nanoTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acknowledge Ledger: {}, Entry: {}", ledgerId, entryId);
            }
            journalAddEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            callbackTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            recycle();
        }

        private Object getCtx() {
            return ctx;
        }

        private final Handle<QueueEntry> recyclerHandle;

        private QueueEntry(Handle<QueueEntry> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<QueueEntry> RECYCLER = new Recycler<QueueEntry>() {
            @Override
            protected QueueEntry newObject(Recycler.Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private void recycle() {
            this.entry = null;
            this.cb = null;
            this.ctx = null;
            this.journalAddEntryStats = null;
            this.callbackTime = null;
            recyclerHandle.recycle(this);
        }
    }

    /**
     * Token which represents the need to force a write to the Journal.
     */
    @VisibleForTesting
    public static class ForceWriteRequest {
        private JournalChannel logFile;
        private RecyclableArrayList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private long lastFlushedPosition;
        private long logId;
        private boolean flushed;

        public int process(ObjectHashSet<BookieRequestHandler> writeHandlers) {
            closeFileIfNecessary();

            // Notify the waiters that the force write succeeded
            for (int i = 0; i < forceWriteWaiters.size(); i++) {
                QueueEntry qe = forceWriteWaiters.get(i);
                if (qe != null) {
                    if (qe.getCtx() instanceof BookieRequestHandler
                            && qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                        writeHandlers.add((BookieRequestHandler) qe.getCtx());
                    }
                    qe.run();
                }
            }

            return forceWriteWaiters.size();
        }

        private void flushFileToDisk() throws IOException {
            if (!flushed) {
                logFile.forceWrite(false);
                flushed = true;
            }
        }

        public void closeFileIfNecessary() {
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    flushFileToDisk();
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                } catch (IOException ioe) {
                    LOG.error("I/O exception while closing file", ioe);
                }
            }
        }

        private final Handle<ForceWriteRequest> recyclerHandle;

        private ForceWriteRequest(Handle<ForceWriteRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private void recycle() {
            logFile = null;
            flushed = false;
            if (forceWriteWaiters != null) {
                forceWriteWaiters.recycle();
                forceWriteWaiters = null;
            }
            recyclerHandle.recycle(this);
        }
    }

    private ForceWriteRequest createForceWriteRequest(JournalChannel logFile,
                          long logId,
                          long lastFlushedPosition,
                          RecyclableArrayList<QueueEntry> forceWriteWaiters,
                          boolean shouldClose) {
        ForceWriteRequest req = forceWriteRequestsRecycler.get();
        req.forceWriteWaiters = forceWriteWaiters;
        req.logFile = logFile;
        req.logId = logId;
        req.lastFlushedPosition = lastFlushedPosition;
        req.shouldClose = shouldClose;
        journalStats.getForceWriteQueueSize().inc();
        return req;
    }

    private static final Recycler<ForceWriteRequest> forceWriteRequestsRecycler = new Recycler<ForceWriteRequest>() {
                @Override
                protected ForceWriteRequest newObject(
                        Recycler.Handle<ForceWriteRequest> handle) {
                    return new ForceWriteRequest(handle);
                }
            };

    /**
     * ForceWriteThread is a background thread which makes the journal durable periodically.
     *
     */
    private class ForceWriteThread extends BookieCriticalThread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Consumer<Void> threadToNotifyOnEx;

        // should we group force writes
        private final boolean enableGroupForceWrites;
        private final Counter forceWriteThreadTime;

        public ForceWriteThread(Consumer<Void> threadToNotifyOnEx,
                                boolean enableGroupForceWrites,
                                StatsLogger statsLogger) {
            super("ForceWriteThread");
            this.setPriority(Thread.MAX_PRIORITY);
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
            this.forceWriteThreadTime = statsLogger.getThreadScopedCounter("force-write-thread-time");
        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");
            ThreadRegistry.register(super.getName());

            if (conf.isBusyWaitEnabled()) {
                try {
                    CpuAffinity.acquireCore();
                } catch (Exception e) {
                    LOG.warn("Unable to acquire CPU core for Journal ForceWrite thread: {}", e.getMessage(), e);
                }
            }

            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>();
            final ForceWriteRequest[] localRequests = new ForceWriteRequest[conf.getJournalQueueSize()];

            while (running) {
                try {
                    int numEntriesInLastForceWrite = 0;

                    int requestsCount = forceWriteRequests.takeAll(localRequests);

                    journalStats.getForceWriteQueueSize().addCount(-requestsCount);

                    // Sync and mark the journal up to the position of the last entry in the batch
                    ForceWriteRequest lastRequest = localRequests[requestsCount - 1];
                    syncJournal(lastRequest);

                    // All the requests in the batch are now fully-synced. We can trigger sending the
                    // responses
                    for (int i = 0; i < requestsCount; i++) {
                        ForceWriteRequest req = localRequests[i];
                        numEntriesInLastForceWrite += req.process(writeHandlers);
                        localRequests[i] = null;
                        req.recycle();
                    }

                    journalStats.getForceWriteGroupingCountStats()
                            .registerSuccessfulValue(numEntriesInLastForceWrite);
                    writeHandlers.forEach(
                            (ObjectProcedure<? super BookieRequestHandler>)
                                    BookieRequestHandler::flushPendingResponse);
                    writeHandlers.clear();
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("ForceWrite thread interrupted");
                    running = false;
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.accept(null);
        }

        private void syncJournal(ForceWriteRequest lastRequest) throws IOException {
            long fsyncStartTime = MathUtils.nowInNano();
            try {
                lastRequest.flushFileToDisk();
                journalStats.getJournalSyncStats().registerSuccessfulEvent(MathUtils.elapsedNanos(fsyncStartTime),
                        TimeUnit.NANOSECONDS);
                lastLogMark.setCurLogMark(lastRequest.logId, lastRequest.lastFlushedPosition);
            } catch (IOException ioe) {
                journalStats.getJournalSyncStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(fsyncStartTime), TimeUnit.NANOSECONDS);
                throw ioe;
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    static final int PADDING_MASK = -0x100;

    static void writePaddingBytes(JournalChannel jc, ByteBuf paddingBuffer, int journalAlignSize)
            throws IOException {
        int bytesToAlign = (int) (jc.bc.position() % journalAlignSize);
        if (0 != bytesToAlign) {
            int paddingBytes = journalAlignSize - bytesToAlign;
            if (paddingBytes < 8) {
                paddingBytes = journalAlignSize - (8 - paddingBytes);
            } else {
                paddingBytes -= 8;
            }
            paddingBuffer.clear();
            // padding mask
            paddingBuffer.writeInt(PADDING_MASK);
            // padding len
            paddingBuffer.writeInt(paddingBytes);
            // padding bytes
            paddingBuffer.writerIndex(paddingBuffer.writerIndex() + paddingBytes);

            jc.preAllocIfNeeded(paddingBuffer.readableBytes());
            // write padding bytes
            jc.bc.write(paddingBuffer);
        }
    }

    static final long MB = 1024 * 1024L;
    static final int KB = 1024;
    // max journal file size
    final long maxJournalSize;
    // pre-allocation size for the journal files
    final long journalPreAllocSize;
    // write buffer size for the journal files
    final int journalWriteBufferSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;
    final ServerConfiguration conf;
    final ForceWriteThread forceWriteThread;
    final FileChannelProvider fileChannelProvider;

    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInNanos;
    // Threshold after which we flush any buffered journal entries
    private final long bufferedEntriesThreshold;
    // Threshold after which we flush any buffered journal writes
    private final long bufferedWritesThreshold;
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty;
    // should we hint the filesystem to remove pages from cache after force write
    private final boolean removePagesFromCache;
    private final int journalFormatVersionToWrite;
    private final int journalAlignmentSize;
    // control PageCache flush interval when syncData disabled to reduce disk io util
    private final long journalPageCacheFlushIntervalMSec;
    // Whether reuse journal files, it will use maxBackupJournal as the journal file pool.
    private final boolean journalReuseFiles;

    // Should data be fsynced on disk before triggering the callback
    private final boolean syncData;

    private final LastLogMark lastLogMark = new LastLogMark(0, 0);

    private static final String LAST_MARK_DEFAULT_NAME = "lastMark";

    private final String lastMarkFileName;

    private final Counter callbackTime;
    private static final String journalThreadName = "BookieJournal";

    // journal entry queue to commit
    final BatchedBlockingQueue<QueueEntry> queue;
    BatchedBlockingQueue<ForceWriteRequest> forceWriteRequests;

    volatile boolean running = true;
    private final LedgerDirsManager ledgerDirsManager;
    private final ByteBufAllocator allocator;

    // Expose Stats
    private final JournalStats journalStats;

    private JournalAliveListener journalAliveListener;

    private MemoryLimitController memoryLimitController;


    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT);
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger, ByteBufAllocator allocator) {
        this.allocator = allocator;

        StatsLogger journalStatsLogger = statsLogger.scopeLabel("journalIndex", String.valueOf(journalIndex));

        if (conf.isBusyWaitEnabled()) {
            // To achieve lower latency, use busy-wait blocking queue implementation
            queue = new BlockingMpscQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BlockingMpscQueue<>(conf.getJournalQueueSize());
        } else {
            queue = new BatchedArrayBlockingQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BatchedArrayBlockingQueue<>(conf.getJournalQueueSize());
        }

        // Adjust the journal max memory in case there are multiple journals configured.
        long journalMaxMemory = conf.getJournalMaxMemorySizeMb() / conf.getJournalDirNames().length * 1024 * 1024;
        this.memoryLimitController = new MemoryLimitController(journalMaxMemory);
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        this.journalDirectory = journalDirectory;
        this.maxJournalSize = conf.getMaxJournalSizeMB() * MB;
        this.journalPreAllocSize = conf.getJournalPreAllocSizeMB() * MB;
        this.journalWriteBufferSize = conf.getJournalWriteBufferSizeKB() * KB;
        this.syncData = conf.getJournalSyncData();
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.forceWriteThread = new ForceWriteThread((__) -> this.interruptThread(),
                conf.getJournalAdaptiveGroupWrites(), journalStatsLogger);
        this.maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(conf.getJournalMaxGroupWaitMSec());
        this.bufferedWritesThreshold = conf.getJournalBufferedWritesThreshold();
        this.bufferedEntriesThreshold = conf.getJournalBufferedEntriesThreshold();
        this.journalFormatVersionToWrite = conf.getJournalFormatVersionToWrite();
        this.journalAlignmentSize = conf.getJournalAlignmentSize();
        this.journalPageCacheFlushIntervalMSec = conf.getJournalPageCacheFlushIntervalMSec();
        this.journalReuseFiles = conf.getJournalReuseFiles();
        this.callbackTime = journalStatsLogger.getThreadScopedCounter("callback-time");
        // Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = maxGroupWaitInNanos <= 0 || conf.getJournalFlushWhenQueueEmpty();

        this.removePagesFromCache = conf.getJournalRemovePagesFromCache();
        // read last log mark
        if (conf.getJournalDirs().length == 1) {
            lastMarkFileName = LAST_MARK_DEFAULT_NAME;
        } else {
            lastMarkFileName = LAST_MARK_DEFAULT_NAME + "." + journalIndex;
        }
        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : {}", lastLogMark.getCurMark());
        }

        try {
            this.fileChannelProvider = FileChannelProvider.newProvider(conf.getJournalChannelProvider());
        } catch (IOException e) {
            LOG.error("Failed to initiate file channel provider: {}", conf.getJournalChannelProvider());
            throw new RuntimeException(e);
        }

        // Expose Stats
        this.journalStats = new JournalStats(journalStatsLogger, journalMaxMemory,
                () -> memoryLimitController.currentUsage());
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
                   LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
                   ByteBufAllocator allocator, JournalAliveListener journalAliveListener) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator);
        this.journalAliveListener = journalAliveListener;
    }

    @VisibleForTesting
    static Journal newJournal(int journalIndex, File journalDirectory, ServerConfiguration conf,
                                     LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
                                     ByteBufAllocator allocator, JournalAliveListener journalAliveListener) {
        return new Journal(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator,
                journalAliveListener);
    }

    JournalStats getJournalStats() {
        return this.journalStats;
    }

    public File getJournalDirectory() {
        return journalDirectory;
    }

    public LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    /**
     * Update lastLogMark of the journal
     * Indicates that the file has been processed.
     * @param id
     * @param scanOffset
     */
    void setLastLogMark(Long id, long scanOffset) {
        lastLogMark.setCurLogMark(id, scanOffset);
    }

    /**
     * Application tried to schedule a checkpoint. After all the txns added
     * before checkpoint are persisted, a <i>checkpoint</i> will be returned
     * to application. Application could use <i>checkpoint</i> to do its logic.
     */
    @Override
    public Checkpoint newCheckpoint() {
        return new LogMarkCheckpoint(lastLogMark.markLog());
    }

    /**
     * Telling journal a checkpoint is finished.
     *
     * @throws IOException
     */
    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        if (!(checkpoint instanceof LogMarkCheckpoint)) {
            return; // we didn't create this checkpoint, so dont do anything with it
        }
        LogMarkCheckpoint lmcheckpoint = (LogMarkCheckpoint) checkpoint;
        LastLogMark mark = lmcheckpoint.mark;

        mark.rollLog(mark);
        if (compact) {
            // list the journals that have been marked
            List<Long> logs = listJournalIds(journalDirectory, new JournalRollingFilter(mark));
            // keep MAX_BACKUP_JOURNALS journal files before marked journal
            if (logs.size() >= maxBackupJournals) {
                int maxIdx = logs.size() - maxBackupJournals;
                for (int i = 0; i < maxIdx; i++) {
                    long id = logs.get(i);
                    // make sure the journal id is smaller than marked journal id
                    if (id < mark.getCurMark().getLogFileId()) {
                        File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                        if (!journalFile.delete()) {
                            LOG.warn("Could not delete old journal file {}", journalFile);
                        }
                        LOG.info("garbage collected journal " + journalFile.getName());
                    }
                }
            }
        }
    }

    /**
     * Scan the journal.
     *
     * @param journalId         Journal Log Id
     * @param journalPos        Offset to start scanning
     * @param scanner           Scanner to handle entries
     * @param skipInvalidRecord when invalid record,should we skip it or not
     * @return scanOffset - represents the byte till which journal was read
     * @throws IOException
     */
    public long scanJournal(long journalId, long journalPos, JournalScanner scanner, boolean skipInvalidRecord)
        throws IOException {
        JournalChannel recLog;
        if (journalPos <= 0) {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                conf, fileChannelProvider);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                    journalPos, conf, fileChannelProvider);
        }
        int journalVersion = recLog.getFormatVersion();
        try {
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            ByteBuffer recBuff = ByteBuffer.allocate(64 * 1024);
            while (true) {
                // entry start offset
                long offset = recLog.fc.position();
                // start reading entry
                lenBuff.clear();
                fullRead(recLog, lenBuff);
                if (lenBuff.remaining() != 0) {
                    break;
                }
                lenBuff.flip();
                int len = lenBuff.getInt();
                if (len == 0) {
                    break;
                }
                boolean isPaddingRecord = false;
                if (len < 0) {
                    if (len == PADDING_MASK && journalVersion >= JournalChannel.V5) {
                        // skip padding bytes
                        lenBuff.clear();
                        fullRead(recLog, lenBuff);
                        if (lenBuff.remaining() != 0) {
                            break;
                        }
                        lenBuff.flip();
                        len = lenBuff.getInt();
                        if (len == 0) {
                            continue;
                        }
                        isPaddingRecord = true;
                    } else {
                        LOG.error("Invalid record found with negative length: {}", len);
                        throw new IOException("Invalid record found with negative length " + len);
                    }
                }
                recBuff.clear();
                if (recBuff.remaining() < len) {
                    recBuff = ByteBuffer.allocate(len);
                }
                recBuff.limit(len);
                if (fullRead(recLog, recBuff) != len) {
                    // This seems scary, but it just means that this is where we
                    // left off writing
                    break;
                }
                recBuff.flip();
                if (!isPaddingRecord) {
                    scanner.process(journalVersion, offset, recBuff);
                }
            }
            return recLog.fc.position();
        } catch (IOException e) {
            if (skipInvalidRecord) {
                LOG.warn("Failed to parse journal file, and skipInvalidRecord is true, skip this journal file reply");
            } else {
                throw e;
            }
            return recLog.fc.position();
        } finally {
            recLog.close();
        }
    }

    /**
     * record an add entry operation in journal.
     */
    public void logAddEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        logAddEntry(ledgerId, entryId, entry, ackBeforeSync, cb, ctx);
    }

    @VisibleForTesting
    public void logAddEntry(long ledgerId, long entryId, ByteBuf entry,
                            boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        // Retain entry until it gets written to journal
        entry.retain();

        journalStats.getJournalQueueSize().inc();

        memoryLimitController.reserveMemory(entry.readableBytes());

        queue.put(QueueEntry.create(
                entry, ackBeforeSync, ledgerId, entryId, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalAddEntryStats(),
                callbackTime));
    }

    void forceLedger(long ledgerId, WriteCallback cb, Object ctx) {
        queue.add(QueueEntry.create(
                null, false /* ackBeforeSync */, ledgerId,
                BookieImpl.METAENTRY_ID_FORCE_LEDGER, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalForceLedgerStats(),
                callbackTime));
        // Increment afterwards because the add operation could fail.
        journalStats.getJournalQueueSize().inc();
    }

    /**
     * Get the length of journal entries queue.
     *
     * @return length of journal entry queue.
     */
    public int getJournalQueueLength() {
        return queue.size();
    }

    @VisibleForTesting
    JournalChannel newLogFile(long logId, Long replaceLogId) throws IOException {
        return new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize,
                journalAlignmentSize, removePagesFromCache,
                journalFormatVersionToWrite, getBufferedChannelBuilder(),
                conf, fileChannelProvider, replaceLogId);
    }

    /**
     * Journal线程：用于将日志条目持久化到journal文件
     *
     * <p>
     * 不仅负责journal条目的持久化，也负责在journal文件达到大小上限时进行文件滚动（rollover）。
     * </p>
     *
     * <p>
     * 滚动journal文件时，先关闭当前写入journal，生成一个新的journal文件（时间戳命名），继续后续持久化流程。
     * 已完成的journal文件会被SyncThread线程（后台元数据同步线程）回收。
     * </p>
     *
     * @see org.apache.bookkeeper.bookie.SyncThread
     */
    public void run() {
        LOG.info("Starting journal on {}", journalDirectory);
        ThreadRegistry.register(journalThreadName); // 注册线程到线程管理器，便于故障排查和线程跟踪

        // 是否开启CPU绑定等待（优化线程上下文切换性能，典型用于低延迟场景如SSD）
        if (conf.isBusyWaitEnabled()) {
            try {
                CpuAffinity.acquireCore();
            } catch (Exception e) {
                LOG.warn("Unable to acquire CPU core for Journal thread: {}", e.getMessage(), e);
            }
        }

        // 初始化各类缓冲对象
        RecyclableArrayList<QueueEntry> toFlush = entryListRecycler.newInstance(); // flush队列，用于短暂收集待刷盘请求
        int numEntriesToFlush = 0;        // 当前待刷盘队列条目数
        ByteBuf lenBuff = Unpooled.buffer(4); // 日志条目长度缓冲区
        ByteBuf paddingBuff = Unpooled.buffer(2 * conf.getJournalAlignmentSize());
        paddingBuff.writeZero(paddingBuff.capacity());

        BufferedChannel bc = null; // 日志写入底层缓存通道
        JournalChannel logFile = null; // 当前journal文件句柄
        forceWriteThread.start(); // 启动专用强制刷盘线程
        Stopwatch journalCreationWatcher = Stopwatch.createUnstarted(); // 用于统计journal文件创建时长
        Stopwatch journalFlushWatcher = Stopwatch.createUnstarted(); // 用于统计flush时长
        long batchSize = 0; // 当前批次写入字节总量

        try {
            // 启动时，获取所有已有的journal文件ID列表
            List<Long> journalIds = listJournalIds(journalDirectory, null);
            // 日志文件ID初始化：如无旧文件，则用当前时间戳，否则用最后一个journal文件ID
            // 注意不要用nanoTime(), 只适合测量耗时不适合生成全局唯一ID
            long logId = journalIds.isEmpty() ? System.currentTimeMillis() : journalIds.get(journalIds.size() - 1);
            long lastFlushPosition = 0;       // 上次刷盘的位置
            boolean groupWhenTimeout = false; // 是否进入批量刷盘超时分组流程

            long dequeueStartTime = 0L;       // dequeue操作的起始时间，用于性能统计
            long lastFlushTimeMs = System.currentTimeMillis(); // 上次刷盘时间点

            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>(); // 批量收集待刷盘回调handler
            QueueEntry[] localQueueEntries = new QueueEntry[conf.getJournalQueueSize()];
            int localQueueEntriesIdx = 0;     // 本地队列指针
            int localQueueEntriesLen = 0;     // 本地队列长度
            QueueEntry qe = null; // 当前处理的入队Journal条目

            // 主journal处理循环
            while (true) {
                // -------- 1. Journal文件创建或滚动 ---------
                if (null == logFile) {
                    logId = logId + 1; // 新journal文件ID递增
                    journalIds = listJournalIds(journalDirectory, null);
                    // 如果复用journal文件支持、数量超限并且最旧ID还在lastLogMark之前，则选择复用最老文件ID
                    Long replaceLogId = fileChannelProvider.supportReuseFile() && journalReuseFiles
                            && journalIds.size() >= maxBackupJournals
                            && journalIds.get(0) < lastLogMark.getCurMark().getLogFileId()
                            ? journalIds.get(0) : null;

                    // 创建journal文件计时
                    journalCreationWatcher.reset().start();
                    logFile = newLogFile(logId, replaceLogId); // 创建新journal文件或复用老文件
                    journalStats.getJournalCreationStats().registerSuccessfulEvent(
                            journalCreationWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                    bc = logFile.getBufferedChannel(); // 获取journal底层写缓冲通道
                    lastFlushPosition = bc.position(); // 初始化刷盘位置
                }

                // -------- 2. 获取待处理JournalEntry条目（出队） --------
                if (qe == null) {
                    if (dequeueStartTime != 0) {
                        // 记录队列等待时间
                        journalStats.getJournalProcessTimeStats()
                                .registerSuccessfulEvent(MathUtils.elapsedNanos(dequeueStartTime), TimeUnit.NANOSECONDS);
                    }

                    // 本地队列每次清空，重新拉取queue
                    localQueueEntriesIdx = 0;
                    if (numEntriesToFlush == 0) {
                        // 如果没有pending entry，则一直阻塞等新条目到来
                        localQueueEntriesLen = queue.takeAll(localQueueEntries);
                    } else {
                        // 有pending entry，调整poll等待时长，最多等maxGroupWaitInNanos
                        long pollWaitTimeNanos = maxGroupWaitInNanos
                                - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            pollWaitTimeNanos = 0;
                        }
                        // 指定短超时轮询
                        localQueueEntriesLen = queue.pollAll(localQueueEntries,
                                pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                    }

                    dequeueStartTime = MathUtils.nowInNano(); // 更新下次出队起始计时点

                    // 若队列有新条目，选取第一个
                    if (localQueueEntriesLen > 0) {
                        qe = localQueueEntries[localQueueEntriesIdx];
                        localQueueEntries[localQueueEntriesIdx++] = null;
                    }
                }

                // -------- 3. 判断是否触发journal刷盘 --------
                if (numEntriesToFlush > 0) {
                    boolean shouldFlush = false;
                    // Journal刷盘的触发条件有三种（只要满足任何之一就需要刷盘）：
                    // 1. oldest pending entry超时等待
                    if (maxGroupWaitInNanos > 0 && !groupWhenTimeout && (MathUtils
                            .elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                        groupWhenTimeout = true;
                    } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout
                            && (qe == null // 没有新entry可分组
                            || MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos)) {
                        // 没有新条目延迟或者已达到批量分组超时阈值
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushMaxWaitCounter().inc();
                    } else if (qe != null
                            && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold)
                            || (bc.position() > lastFlushPosition + bufferedWritesThreshold))) {
                        // 2. buffered entry数量超出阈值或buffer写入字节超阈，需要刷盘
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushMaxOutstandingBytesCounter().inc();
                    } else if (qe == null && flushWhenQueueEmpty) {
                        // 3. Journal队列为空且允许“空队列刷盘”，适用于测试或低吞吐场景
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushEmptyQueueCounter().inc();
                    }

                    // 刷盘处理
                    if (shouldFlush) {
                        // V5及以上版本，写对齐填充字节
                        if (journalFormatVersionToWrite >= JournalChannel.V5) {
                            writePaddingBytes(logFile, paddingBuff, journalAlignmentSize);
                        }
                        journalFlushWatcher.reset().start();
                        bc.flush(); // 刷写内存到文件

                        // 处理toFlush中的所有entry：完成回调、释放资源
                        for (int i = 0; i < toFlush.size(); i++) {
                            QueueEntry entry = toFlush.get(i);
                            if (entry != null && (!syncData || entry.ackBeforeSync)) {
                                toFlush.set(i, null);
                                numEntriesToFlush--;
                                if (entry.getCtx() instanceof BookieRequestHandler
                                        && entry.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                                    writeHandlers.add((BookieRequestHandler) entry.getCtx());
                                }
                                entry.run(); // 回调
                            }
                        }
                        // 刷新所有pending response
                        writeHandlers.forEach(
                                (ObjectProcedure<? super BookieRequestHandler>)
                                        BookieRequestHandler::flushPendingResponse);
                        writeHandlers.clear();

                        lastFlushPosition = bc.position();
                        journalStats.getJournalFlushStats().registerSuccessfulEvent(
                                journalFlushWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                        // 如果debug模式，打印刷盘条目信息
                        if (LOG.isDebugEnabled()) {
                            for (QueueEntry e : toFlush) {
                                if (e != null && LOG.isDebugEnabled()) {
                                    LOG.debug("Written and queuing for flush Ledger: {}  Entry: {}",
                                            e.ledgerId, e.entryId);
                                }
                            }
                        }

                        // 刷盘统计
                        journalStats.getForceWriteBatchEntriesStats()
                                .registerSuccessfulValue(numEntriesToFlush);
                        journalStats.getForceWriteBatchBytesStats()
                                .registerSuccessfulValue(batchSize);

                        // 检查journal是否需要rollover（文件太大）
                        boolean shouldRolloverJournal = (lastFlushPosition > maxJournalSize);
                        // 三种情形触发强制数据同步到磁盘（底层force write）：
                        // 1. journalSyncData启用（目标为高可靠/高性能SSD场景）
                        // 2. 文件大小超过最大限制
                        // 3. 普通情况下，周期性触发，根据journalPageCacheFlushIntervalMSec控制间隔
                        if (syncData
                                || shouldRolloverJournal
                                || (System.currentTimeMillis() - lastFlushTimeMs
                                >= journalPageCacheFlushIntervalMSec)) {
                            // 放入刷盘请求队列，交给forceWriteThread处理
                            forceWriteRequests.put(createForceWriteRequest(logFile, logId, lastFlushPosition,
                                    toFlush, shouldRolloverJournal));
                            lastFlushTimeMs = System.currentTimeMillis();
                        }
                        toFlush = entryListRecycler.newInstance(); // 刷完新建队列
                        numEntriesToFlush = 0;
                        batchSize = 0L;

                        // 文件rollover时，关闭当前journal文件，进入新一轮创建流程
                        if (shouldRolloverJournal) {
                            logFile = null;
                            continue;
                        }
                    }
                }

                // -------- 4. 检查线程关闭 --------
                if (!running) {
                    LOG.info("Journal Manager is asked to shut down, quit.");
                    break;
                }

                // -------- 5. 若无新queue entry，则进入下轮循环 --------
                if (qe == null) { // no more queue entry
                    continue;
                }

                // 统计journal队列的大小和等待耗时
                journalStats.getJournalQueueSize().dec();
                journalStats.getJournalQueueStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);

                // -------- 6. 按Journal格式和entry类型处理写入条目 --------
                if ((qe.entryId == BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC)
                        && (journalFormatVersionToWrite < JournalChannel.V6)) {
                    /*
                     * 代码新版本支持explicitLac持久化，但实际journal文件格式老于V6，不允许写入该特殊entry
                     * 所以直接释放资源、计数器，跳过写入
                     */
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    ReferenceCountUtil.release(qe.entry);
                } else if (qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                    // 非“强制metaentry/forceLedger”类型，正常写入journal文件
                    int entrySize = qe.entry.readableBytes();
                    journalStats.getJournalWriteBytes().addCount(entrySize);
                    batchSize += (4 + entrySize);

                    lenBuff.clear();
                    lenBuff.writeInt(entrySize);

                    // 预分配journal文件空间
                    logFile.preAllocIfNeeded(4 + entrySize);

                    bc.write(lenBuff); // 先写长度
                    bc.write(qe.entry); // 再写内容本身
                    // 释放内存页计数与引用
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    ReferenceCountUtil.release(qe.entry);
                }

                // -------- 7. 将条目加入pending flush队列 --------
                toFlush.add(qe);
                numEntriesToFlush++;

                // -------- 8. move to next local queue entry --------
                if (localQueueEntriesIdx < localQueueEntriesLen) {
                    qe = localQueueEntries[localQueueEntriesIdx];
                    localQueueEntries[localQueueEntriesIdx++] = null;
                } else {
                    qe = null;
                }
            }
        } catch (IOException ioe) {
            LOG.error("I/O exception in Journal thread!", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Journal exits when shutting down");
        } finally {
            // 清理资源。关闭底层BufferedChannel，通知journal退出监听器
            IOUtils.close(LOG, bc);
            if (journalAliveListener != null) {
                journalAliveListener.onJournalExit();
            }
        }
        LOG.info("Journal exited loop!");
    }


    public BufferedChannelBuilder getBufferedChannelBuilder() {
        return (FileChannel fc, int capacity) -> new BufferedChannel(allocator, fc, capacity);
    }

    /**
     * Shuts down the journal.
     */
    public synchronized void shutdown() {
        try {
            if (!running) {
                return;
            }
            LOG.info("Shutting down Journal");
            if (fileChannelProvider != null) {
                fileChannelProvider.close();
            }

            forceWriteThread.shutdown();

            running = false;
            this.interruptThread();
            this.joinThread();
            LOG.info("Finished Shutting down Journal thread");
        } catch (IOException | InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted during shutting down journal : ", ie);
        }
    }

    private static int fullRead(JournalChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while (bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }

    /**
     * Wait for the Journal thread to exit.
     * This is method is needed in order to mock the journal, we can't mock final method of java.lang.Thread class
     *
     * @throws InterruptedException
     */
    @VisibleForTesting
    public void joinThread() throws InterruptedException {
        if (thread != null) {
            thread.join();
        }
    }

    public void interruptThread() {
        if (thread != null) {
            thread.interrupt();
        }
    }

    public synchronized void start() {
        thread = new BookieCriticalThread(() -> run(), journalThreadName + "-" + conf.getBookiePort());
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    long getMemoryUsage() {
        return memoryLimitController.currentUsage();
    }

    @VisibleForTesting
    void setMemoryLimitController(MemoryLimitController memoryLimitController) {
        this.memoryLimitController = memoryLimitController;
    }

    @VisibleForTesting
    public void setForceWriteRequests(BatchedBlockingQueue<ForceWriteRequest> forceWriteRequests) {
        this.forceWriteRequests = forceWriteRequests;
    }
}
