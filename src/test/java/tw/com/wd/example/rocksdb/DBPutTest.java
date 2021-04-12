package tw.com.wd.example.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;


public class DBPutTest {
    private static final String PUT_DATA_1 = "just-for-test";
    private static final String PUT_DATA_1_1 = "for-test";
    private static final String PUT_DATA_1_2 = "test";
    private static final String PUT_DATA_2 = "test-from-ByteBuffer";
    private static final String PUT_DATA_3 = "test-for-cf";
    private static final String CF_TEST = "cf_test";
    private String dbPath;
    private DBOptions options;
    private RocksDB rocksDB;

    @Before
    public void beforeTest() {
        // Setup path of RocksDB
        this.dbPath = DBConfig.getDBRootPath() + File.separator + DBOpenTest.class.getSimpleName();

        // Setup option of RocksDB
        this.options = new DBOptions();
        this.options.setCreateIfMissing(true);
        this.options.setCreateMissingColumnFamilies(true);

        try {

            final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                    new ColumnFamilyDescriptor(CF_TEST.getBytes()));

            final List<ColumnFamilyHandle> columnFamilyHandleList =
                    new ArrayList<>();

            rocksDB = RocksDB.open(this.options, this.dbPath, cfNames, columnFamilyHandleList);
        } catch (RocksDBException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @After
    public void afterTest() {
        if (this.rocksDB != null) {
            this.rocksDB.close();
        }
    }

    @Test
    public void testCommonPut() throws RocksDBException {
        Exception exception = null;

        try {
            this.rocksDB.put(PUT_DATA_1.getBytes(), PUT_DATA_1.getBytes());
            this.rocksDB.put(PUT_DATA_1.getBytes(), 5, PUT_DATA_1.length() - 5, PUT_DATA_1.getBytes(), 9, 4);
        } catch (Exception e) {
            e.printStackTrace();
            exception = e;
        }

        assertThat(exception, is(nullValue()));
        assertThat(this.rocksDB.get(PUT_DATA_1.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(PUT_DATA_1.getBytes())), is(PUT_DATA_1));
        assertThat(this.rocksDB.get(PUT_DATA_1_1.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(PUT_DATA_1_1.getBytes())), is(PUT_DATA_1_2));
    }

    @Test
    public void testPutWithWriteOptions() throws RocksDBException {
        Exception exception = null;
        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect(PUT_DATA_2.getBytes().length*2);
        testByteBuffer.put(PUT_DATA_2.getBytes());
        testByteBuffer.flip();

        try {
            WriteOptions writeOptions = new WriteOptions();

            this.rocksDB.put(writeOptions, PUT_DATA_1.getBytes(), PUT_DATA_1.getBytes());
            this.rocksDB.put(writeOptions, PUT_DATA_1.getBytes(), 5, PUT_DATA_1.length() - 5, PUT_DATA_1.getBytes(), 9, 4);
            this.rocksDB.put(writeOptions, testByteBuffer, testByteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
            exception = e;
        }

        assertThat(exception, is(nullValue()));
        assertThat(this.rocksDB.get(PUT_DATA_1.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(PUT_DATA_1.getBytes())), is(PUT_DATA_1));
        assertThat(this.rocksDB.get(PUT_DATA_1_1.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(PUT_DATA_1_1.getBytes())), is(PUT_DATA_1_2));
        assertThat(new String(this.rocksDB.get(PUT_DATA_2.getBytes())), is(PUT_DATA_2));
    }

    @Test
    public void testPutWithColumnFamily() throws RocksDBException {
        Exception exception = null;
        ColumnFamilyHandle columnFamilyHandle = null;

        try {
            ColumnFamilyHandle defaultColumnFamilyHandle = this.rocksDB.getDefaultColumnFamily();

            ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(CF_TEST.getBytes(StandardCharsets.UTF_8));
            columnFamilyHandle = this.rocksDB.createColumnFamily(columnFamilyDescriptor);

            this.rocksDB.put(defaultColumnFamilyHandle, PUT_DATA_1.getBytes(StandardCharsets.UTF_8), PUT_DATA_1.getBytes(StandardCharsets.UTF_8));
            this.rocksDB.put(columnFamilyHandle, PUT_DATA_3.getBytes(StandardCharsets.UTF_8), PUT_DATA_3.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            exception = e;
            exception.printStackTrace();
        }

        assertThat(exception, is(nullValue()));
        assertThat(this.rocksDB.get(PUT_DATA_1.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(PUT_DATA_1.getBytes())), is(PUT_DATA_1));
        assertThat(columnFamilyHandle, is(notNullValue()));
        assertThat(this.rocksDB.get(columnFamilyHandle, PUT_DATA_3.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(columnFamilyHandle, PUT_DATA_3.getBytes())), is(PUT_DATA_3));
    }
 }
