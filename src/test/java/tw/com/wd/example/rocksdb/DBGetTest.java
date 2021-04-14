package tw.com.wd.example.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


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

        List<ColumnFamilyHandle> cfHandleList = new ArrayList<ColumnFamilyHandle>();


        rocksDB = RocksDB.open(dbOptions, dbPath, cfDescriptorList, cfHandleList);
        System.out.printf("Size of ColumnFamily: %d\n", cfHandleList.size());

        for (ColumnFamilyHandle cfHandle : cfHandleList) {
            System.out.printf("\t#%s\n", new String(cfHandle.getName()));
        }

        rocksDB.put(PUT_DATA_1.getBytes(), PUT_DATA_1.getBytes());
        rocksDB.put(cfHandleList.get(1), PUT_DATA_2.getBytes(), PUT_DATA_2.getBytes());
        rocksDB.put(cfHandleList.get(1), PUT_DATA_3.getBytes(), PUT_DATA_3.getBytes());
        rocksDB.put(cfHandleList.get(2), PUT_DATA_4.getBytes(), PUT_DATA_4.getBytes());
    }

    @After
    public void afterTest() {
        if (this.rocksDB != null) {
            this.rocksDB.close();
        }
    }

    @Test
    public void testCommonGet() throws RocksDBException {
        try (RocksDB rocksDB = RocksDB.open(dbPath)) {


        }
    }
}
