package tw.com.wd.example.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;


public class DBColumnFamilyTest {
    private static final String CF_DEFAULT = "default";
    private static final String CF_TEST_1 = "cf_test_1";
    private static final String CF_TEST_2 = "cf_test_2";
    private String dbPath;

    @Before
    public void beforeTest() {
        this.dbPath = DBConfig.getDBRootPath() + File.separator + this.getClass().getSimpleName();
    }

    @After
    public void afterTest() throws RocksDBException {
        RocksDB.destroyDB(this.dbPath, new Options());
    }

    @Test
    public void testCreateColumnFamily() throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.open(this.dbPath)) {
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(CF_TEST_1.getBytes());
            ColumnFamilyHandle cf1Handle = rocksDB.createColumnFamily(cfDescriptor);

            List<byte[]> cfNameList = RocksDB.listColumnFamilies(new Options(), dbPath);

            assertThat(cf1Handle, is(notNullValue()));
            assertThat(cfNameList, is(notNullValue()));
            assertThat(cfNameList.size(), is(2));
            assertThat(new String(cfNameList.get(0)), is(CF_DEFAULT));
            assertThat(new String(cfNameList.get(1)), is(CF_TEST_1));
        }
    }

    @Test
    public void testDropColumnFamily() throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.open(this.dbPath)) {
            List<ColumnFamilyDescriptor> cfDescriptorList = new ArrayList<ColumnFamilyDescriptor>();
            cfDescriptorList.add(new ColumnFamilyDescriptor(CF_TEST_1.getBytes()));
            cfDescriptorList.add(new ColumnFamilyDescriptor(CF_TEST_2.getBytes()));

            List<ColumnFamilyHandle> cfHandleList = rocksDB.createColumnFamilies(cfDescriptorList);
            List<byte[]> cfNameList = RocksDB.listColumnFamilies(new Options(), dbPath);

            assertThat(cfHandleList, is(notNullValue()));
            assertThat(cfHandleList.size(), is(2));
            assertThat(cfNameList, is(notNullValue()));
            assertThat(cfNameList.size(), is(3));

            rocksDB.dropColumnFamilies(cfHandleList);
            cfNameList = RocksDB.listColumnFamilies(new Options(), dbPath);

            assertThat(cfNameList, is(notNullValue()));
            assertThat(cfNameList.size(), is(1));
        }
    }

    @Test
    public void testPutWithoutColumnFamily() throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.open(this.dbPath)) {
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(CF_TEST_1.getBytes());
            ColumnFamilyHandle cf1Handle = rocksDB.createColumnFamily(cfDescriptor);
            rocksDB.put(cf1Handle, "test".getBytes(), "test".getBytes());

            Exception exception = null;
            rocksDB.dropColumnFamily(cf1Handle);
            try {
                rocksDB.put(cf1Handle, "test2".getBytes(), "test2".getBytes());
            } catch (Exception e) {
                exception = e;
            }

            assertThat(exception, is(notNullValue()));
            assertThat(rocksDB.get("test".getBytes()), is(nullValue()));
            assertThat(RocksDB.listColumnFamilies(new Options(), dbPath).size(), is(1));
        }
    }

    @Test
    public void testPutAfterDestoryColumnFamily() throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.open(this.dbPath)) {
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(CF_TEST_1.getBytes());
            ColumnFamilyHandle cf1Handle = rocksDB.createColumnFamily(cfDescriptor);

            Exception exception = null;

            try {
                rocksDB.put(cf1Handle, "test".getBytes(), "test".getBytes());
                rocksDB.destroyColumnFamilyHandle(cf1Handle);
                // Below put operation may occur fatal error from JNI, since ColumnFamilyHandle was destroyed
                // rocksDB.put(cf1Handle, "test2".getBytes(), "test".getBytes());
            } catch (Exception e) {
                exception = e;
            }

            assertThat(exception, is(nullValue()));
            // Below get operation may occur fatal error from JNI, since ColumnFamilyHandle was destroyed
            // assertThat(rocksDB.get(cf1Handle, "test".getBytes()), is(notNullValue()));
            assertThat(RocksDB.listColumnFamilies(new Options(), dbPath).size(), is(2));
        }
    }
}
