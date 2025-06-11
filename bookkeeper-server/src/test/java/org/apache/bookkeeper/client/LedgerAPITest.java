package org.apache.bookkeeper.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerAPITest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperTest.class);
    private static final long INVALID_LEDGERID = -1L;
    private final BookKeeper.DigestType digestType;


    public LedgerAPITest() {
        super(3);
        this.digestType = BookKeeper.DigestType.CRC32;
    }


    @Test
    public void testCreateLedger() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        // 设置zk地址和超时时间
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);

        zkUtil.sleepCluster(200, TimeUnit.MILLISECONDS, l);
        l.await();

        BookKeeper bkClient = new BookKeeper(conf);

        byte[] password = "some-password".getBytes();
        LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.MAC, password);

        for (int i = 0; i < 100; i++) {
            handle.addEntry("entry".getBytes());
        }
        // 读取所有数据
        Enumeration<LedgerEntry> entries =
                handle.readEntries(0, handle.getLastAddConfirmed());

        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            System.out.println("Successfully read entry " + entry.getEntryId());
        }

    }

    @Test
    public void test2() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        // 设置zk地址和超时时间
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);

        zkUtil.sleepCluster(200, TimeUnit.MILLISECONDS, l);
        l.await();

        BookKeeper bkClient = new BookKeeper(conf);

        byte[] password = "some-password".getBytes();
        LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.MAC, password);

        for (int i = 0; i < 100; i++) {
            handle.addEntry("entry".getBytes());
        }
        // 读取未提交的数据
        Enumeration<LedgerEntry> entries =
                handle.readUnconfirmedEntries(0, 999);

        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            System.out.println("Successfully read entry " + entry.getEntryId());
        }


        bkClient.asyncDeleteLedger(handle.ledgerId, new DeleteEntryCallback(), null);
    }

    class DeleteEntryCallback implements AsyncCallback.DeleteCallback {

        @Override
        public void deleteComplete(int rc, Object ctx){
        }
    }

}
