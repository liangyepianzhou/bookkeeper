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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.Before;

/**
 * Test BookieStorage with a threshold.
 */
public class BookieStorageThresholdTest extends BookKeeperClusterTestCase {

    private static final int NUM_BOOKIES = 1;
    private static final int NUM_ENTRIES = 100;
    private static final int ENTRY_SIZE = 1024;

    final String msg;
    DigestType digestType = DigestType.CRC32;

    public BookieStorageThresholdTest() {
        super(NUM_BOOKIES);
        // a dummy message
        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < ENTRY_SIZE; i++) {
            msgSB.append("a");
        }
        msg = msgSB.toString();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseConf.setEntryLogSizeLimit(NUM_ENTRIES * ENTRY_SIZE);
        baseConf.setFlushInterval(500);
        // setting very high intervals for GC intervals, so GC/compaction is not invoked by regular scheduler
        baseConf.setGcWaitTime(60000);
        baseConf.setMinorCompactionInterval(600000);
        baseConf.setMajorCompactionInterval(700000);
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        // set isForceGCAllowWhenNoSpace to true, which will forceGC when a disk is full (or when all disks are full)
        baseConf.setIsForceGCAllowWhenNoSpace(true);
        // keep some lower value for DiskCheckInterval, so DiskChecker checks quite often
        baseConf.setDiskCheckInterval(3000);

        super.setUp();
    }

    LedgerHandle[] prepareData(int numEntryLogs) throws Exception {
        // since an entry log file can hold at most 100 entries
        // first ledger write 2 entries, which is less than low watermark
        int num1 = 2;
        // third ledger write more than high watermark entries
        int num3 = (int) (NUM_ENTRIES * 0.7f);
        // second ledger write remaining entries, which is higher than low watermark
        // and less than high watermark
        int num2 = NUM_ENTRIES - num3 - num1;

        LedgerHandle[] lhs = new LedgerHandle[3];
        for (int i = 0; i < 3; ++i) {
            lhs[i] = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
        }

        for (int n = 0; n < numEntryLogs; n++) {
            for (int k = 0; k < num1; k++) {
                lhs[0].addEntry(msg.getBytes());
            }
            for (int k = 0; k < num2; k++) {
                lhs[1].addEntry(msg.getBytes());
            }
            for (int k = 0; k < num3; k++) {
                lhs[2].addEntry(msg.getBytes());
            }
        }

        return lhs;
    }

    /**
     * A Threshold-based disk checker test.
     */
    public class ThresholdTestDiskChecker extends DiskChecker {

        final AtomicBoolean injectDiskOutOfSpaceException;

        public ThresholdTestDiskChecker(float threshold, float warnThreshold) {
            super(threshold, warnThreshold);
            injectDiskOutOfSpaceException = new AtomicBoolean();
        }

        public void setInjectDiskOutOfSpaceException(boolean setValue) {
            injectDiskOutOfSpaceException.set(setValue);
        }

        @Override
        public float checkDir(File dir) throws DiskErrorException, DiskOutOfSpaceException, DiskWarnThresholdException {
            if (injectDiskOutOfSpaceException.get()) {
                throw new DiskOutOfSpaceException("Injected DiskOutOfSpaceException",
                        baseConf.getDiskUsageThreshold() + 2);
            }
            return super.checkDir(dir);
        }
    }

    @FlakyTest(value = "https://github.com/apache/bookkeeper/issues/1562")
    public void testStorageThresholdCompaction() throws Exception {
        stopAllBookies();
        ServerConfiguration conf = newServerConfiguration();
        File ledgerDir1 = tmpDirs.createNew("ledger", "test1");
        File ledgerDir2 = tmpDirs.createNew("ledger", "test2");
        File journalDir = tmpDirs.createNew("journal", "test");
        String[] ledgerDirNames = new String[]{
                ledgerDir1.getPath(),
                ledgerDir2.getPath()
        };
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setJournalDirName(journalDir.getPath());

        BookieServer server = startAndAddBookie(conf).getServer();
        BookieImpl bookie = (BookieImpl) server.getBookie();
        // since we are going to set dependency injected dirsMonitor, so we need to shutdown
        // the dirsMonitor which was created as part of the initialization of Bookie
        bookie.dirsMonitor.shutdown();

        LedgerDirsManager ledgerDirsManager = ((BookieImpl) bookie).getLedgerDirsManager();

        // flag latches
        final CountDownLatch diskWritable = new CountDownLatch(1);
        final CountDownLatch diskFull = new CountDownLatch(1);
        ledgerDirsManager.addLedgerDirsListener(new LedgerDirsListener() {

            @Override
            public void diskWritable(File disk) {
                diskWritable.countDown();
            }

            @Override
            public void diskFull(File disk) {
                diskFull.countDown();
            }

        });

        // Dependency Injected class
        ThresholdTestDiskChecker thresholdTestDiskChecker = new ThresholdTestDiskChecker(
                baseConf.getDiskUsageThreshold(), baseConf.getDiskUsageWarnThreshold());
        bookie.dirsMonitor = new LedgerDirsMonitor(baseConf, thresholdTestDiskChecker,
                Collections.singletonList(ledgerDirsManager));
        // set the dirsMonitor and initiate/start it
        bookie.dirsMonitor.init();
        bookie.dirsMonitor.start();

        // create ledgers and add fragments
        LedgerHandle[] lhs = prepareData(3);
        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // delete ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        // validating that LedgerDirsListener are not triggered yet
        assertTrue("Disk Full shouldn't have been triggered yet", diskFull.getCount() == 1);
        assertTrue("Disk writable shouldn't have been triggered yet", diskWritable.getCount() == 1);
        // set exception injection to true, so that next time when checkDir of DiskChecker (ThresholdTestDiskChecker) is
        // called it will throw DiskOutOfSpaceException
        thresholdTestDiskChecker.setInjectDiskOutOfSpaceException(true);

        // now we are waiting for diskFull latch count to get to 0.
        // we are waiting for diskCheckInterval period, so that next time when LedgerDirsMonitor monitors diskusage of
        // its directories, it would get DiskOutOfSpaceException and hence diskFull of all LedgerDirsListener would be
        // called.
        diskFull.await(baseConf.getDiskCheckInterval() + 500, TimeUnit.MILLISECONDS);
        // verifying that diskFull of all LedgerDirsListener are invoked, so countdown of diskFull should come down to 0
        assertTrue("Disk Full should have been triggered", diskFull.getCount() == 0);
        // making sure diskWritable of LedgerDirsListener are not invoked yet
        assertTrue("Disk writable shouldn't have been triggered yet", diskWritable.getCount() == 1);
        // waiting momentarily, because transition to Readonly mode happens asynchronously when there are no more
        // writableLedgerDirs
        Thread.sleep(500);
        assertTrue("Bookie should be transitioned to ReadOnly", bookie.isReadOnly());
        // since we set isForceGCAllowWhenNoSpace to true, when the disk is full (or when all disks are full) it does
        // force GC.
        // Because of getWritableLedgerDirsForNewLog, compaction would be able to create newlog and compact even though
        // there are no writableLedgerDirs
        for (File ledgerDir : bookie.getLedgerDirsManager().getAllLedgerDirs()) {
            assertFalse("Found entry log file ([0,1,2].log. They should have been compacted" + ledgerDir,
                    TestUtils.hasLogFiles(ledgerDir.getParentFile(), true, 0, 1, 2));
        }

        try {
            ledgerDirsManager.getWritableLedgerDirs();
            fail("It is expected that there wont be any Writable LedgerDirs and getWritableLedgerDirs "
                    + "is supposed to throw NoWritableLedgerDirException");
        } catch (NoWritableLedgerDirException nowritableDirsException) {
        }

        // disable exception injection
        thresholdTestDiskChecker.setInjectDiskOutOfSpaceException(false);

        // now we are waiting for diskWritable latch count to get to 0.
        // we are waiting for diskCheckInterval period, so that next time when LedgerDirsMonitor monitors diskusage of
        // its directories, it would find writableledgerdirectory and hence diskWritable of all LedgerDirsListener would
        // be called.
        diskWritable.await(baseConf.getDiskCheckInterval() + 500, TimeUnit.MILLISECONDS);
        // verifying that diskWritable of all LedgerDirsListener are invoked, so countdown of diskWritable should come
        // down to 0
        assertTrue("Disk writable should have been triggered", diskWritable.getCount() == 0);
        // waiting momentarily, because transition to ReadWrite mode happens asynchronously when there is new
        // writableLedgerDirectory
        Thread.sleep(500);
        assertFalse("Bookie should be transitioned to ReadWrite", bookie.isReadOnly());
    }

    @org.junit.Test
    public void testOneDirectoryFull() throws Exception {
        // 1. 创建测试目录
        File ledgerDir1 = tmpDirs.createNew("ledger", "test1");
        File ledgerDir2 = tmpDirs.createNew("ledger", "test2");

        // 2. 配置Bookie
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(1000);
        conf.setLedgerDirNames(new String[] { ledgerDir1.getPath(), ledgerDir2.getPath() });
        conf.setDiskCheckInterval(100); // 缩短磁盘检查间隔
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());

        // 3. 启动Bookie并获取内部组件
        Bookie bookie = new TestBookieImpl(conf);
        BookieImpl bookieImpl = (BookieImpl) bookie;
        LedgerDirsManager ledgerDirsManager = bookieImpl.getLedgerDirsManager();

        // 4. 创建自定义磁盘检查器（关键步骤）
        GCTestDiskChecker diskChecker = new GCTestDiskChecker(
                conf.getDiskUsageThreshold(),
                conf.getDiskUsageWarnThreshold()
        );
        // 设置目录状态：dir1满(100%)，dir2正常(50%)
        File[] currentDirectories = BookieImpl.getCurrentDirectories(new File[] { ledgerDir1, ledgerDir2 });
        diskChecker.setUsageMap(currentDirectories[0], 1.0f);  // 100%使用率
        diskChecker.setUsageMap(currentDirectories[1], 0.5f); // 50%使用率

        // 5. 替换Bookie的磁盘检查器
        bookieImpl.dirsMonitor.shutdown(); // 停止默认监控
        bookieImpl.dirsMonitor = new LedgerDirsMonitor(
                conf,
                diskChecker,
                Collections.singletonList(ledgerDirsManager)
        );
        bookieImpl.dirsMonitor.start();

        // 6. 添加磁盘状态监听器
        CountDownLatch dir1Full = new CountDownLatch(1);
        CountDownLatch dir2Writable = new CountDownLatch(1);

        ledgerDirsManager.addLedgerDirsListener(new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                if (disk.equals(currentDirectories[0])) dir1Full.countDown();
            }

            @Override
            public void diskWritable(File disk) {
                if (disk.equals(ledgerDir2)) dir2Writable.countDown();
            }
        });

        // 7. 等待状态更新（确保事件触发）
        assertTrue("dir1未触发满状态", dir1Full.await(3, TimeUnit.SECONDS));

        // 8. 验证目录状态
        List<File> fullDirs = ledgerDirsManager.getFullFilledLedgerDirs();
        List<File> writableDirs = ledgerDirsManager.getWritableLedgerDirs();

        assertTrue("dir1应被标记为满", fullDirs.contains(currentDirectories[0]));
        assertTrue("dir2应保持可写", writableDirs.contains(currentDirectories[1]));
        assertEquals("应只有1个可写目录", 1, writableDirs.size());

        // 9. 验证GC状态
        ((DbLedgerStorage)bookieImpl.ledgerStorage).getLedgerStorageList().forEach(storage -> {
            if (Objects.equals(storage.getCurrentFile(), currentDirectories[0])) {
                assertTrue("dir1应停止小型压缩", storage.isMinorGcSuspended());
                assertTrue("dir1应停止大型压缩", storage.isMajorGcSuspended());
            } else {
                assertFalse("dir2不应停止小型压缩", storage.isMinorGcSuspended());
                assertFalse("dir2不应停止小型压缩", storage.isMajorGcSuspended());
            }
        });

        // 10. 清理
        bookie.shutdown();
    }

    // 自定义磁盘检查器（模拟不同目录的磁盘状态）
    static class GCTestDiskChecker extends DiskChecker {
        private final Map<File, Float> usageMap = new ConcurrentHashMap<>();

        public GCTestDiskChecker(float threshold, float warnThreshold) {
            super(threshold, warnThreshold);
        }

        // 设置目录的模拟使用率
        public void setUsageMap(File dir, float usage) {
            usageMap.put(dir, usage);
        }

        @Override
        public float checkDir(File dir) throws DiskErrorException, DiskWarnThresholdException, DiskOutOfSpaceException {
            Float usage = usageMap.get(dir);
            if (usage == null) {
                return super.checkDir(dir); // 默认行为
            }
            // 根据预设使用率抛出异常
            if (usage >= 1.0) {
                throw new DiskOutOfSpaceException("模拟磁盘满", usage);
            } else if (usage >= 9.0) {
                throw new DiskWarnThresholdException("模拟磁盘警告", usage);
            }
            return usage;
        }
    }

}
