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


public class DBGetTest {
    private static final String CF_TEST_1 = "cf_test_1";
    private static final String CF_TEST_2 = "cf_test_2";
    private static final String PUT_DATA_1 = "test-put-1";
    private static final String PUT_DATA_2 = "test-put-2";
    private static final String PUT_DATA_3 = "test-put-3";
    private static final String PUT_DATA_4 = "test-put-4";
    private String dbPath;
    private Options options;
    private RocksDB rocksDB;
    private List<ColumnFamilyHandle> cfHandleList;

    @Before
    public void beforeTest() throws RocksDBException {
        // Setup path of RocksDB
        this.dbPath = DBConfig.getDBRootPath() + File.separator + getClass().getSimpleName();

        // Setup option of RocksDB
        this.options = new Options();
        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);


        // Setup ColumnFamily
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        ColumnFamilyDescriptor cfDescriptorDefault = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions);
        ColumnFamilyDescriptor cfDescriptor1 = new ColumnFamilyDescriptor(CF_TEST_1.getBytes(), cfOptions);
        ColumnFamilyDescriptor cfDescriptor2 = new ColumnFamilyDescriptor(CF_TEST_2.getBytes(), cfOptions);

        List<ColumnFamilyDescriptor> cfDescriptorList = new ArrayList<ColumnFamilyDescriptor>();
        cfDescriptorList.add(cfDescriptorDefault);
        cfDescriptorList.add(cfDescriptor1);
        cfDescriptorList.add(cfDescriptor2);

        cfHandleList = new ArrayList<ColumnFamilyHandle>();


        rocksDB = RocksDB.open(dbOptions, dbPath, cfDescriptorList, cfHandleList);

        rocksDB.put(PUT_DATA_1.getBytes(), PUT_DATA_1.getBytes());
        rocksDB.put(cfHandleList.get(1), PUT_DATA_2.getBytes(), PUT_DATA_2.getBytes());
        rocksDB.put(cfHandleList.get(1), PUT_DATA_3.getBytes(), PUT_DATA_3.getBytes());
        rocksDB.put(cfHandleList.get(2), PUT_DATA_4.getBytes(), PUT_DATA_4.getBytes());
    }

    @After
    public void afterTest() {
        if (this.rocksDB != null) {
            this.rocksDB.close();

            for (ColumnFamilyHandle cfHandle : cfHandleList) {
                cfHandle.close();
            }
        }

        try {
            RocksDB.destroyDB(this.dbPath, this.options);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCommonGet() {
        Exception exception = null;
        byte[] result1 = null;
        byte[] result2 = null;
        byte[] result3 = null;
        int result4Count = 0;
        byte[] result4 = new byte[4];

        try {
            result1 = this.rocksDB.get(PUT_DATA_1.getBytes());
            result2 = this.rocksDB.get(PUT_DATA_2.getBytes());

            result3 = this.rocksDB.get(PUT_DATA_1.getBytes(), 0, PUT_DATA_1.length());
            result4Count = this.rocksDB.get(PUT_DATA_1.getBytes(), 0, PUT_DATA_1.length(), result4, 0, 4);
        } catch (Exception e) {
            exception = e;
            e.printStackTrace();
        }


        assertThat(exception, is(nullValue()));
        assertThat(result1, is(notNullValue()));
        assertThat(result2, is(nullValue()));
        assertThat(result3, is(notNullValue()));
        assertThat(result4, is(notNullValue()));
        assertThat(result4Count, is(10));
        assertThat(new String(result1), is(PUT_DATA_1));
        assertThat(new String(result3), is(PUT_DATA_1));
        assertThat(new String(result4), is(PUT_DATA_1.substring(0, 4)));
    }
}
