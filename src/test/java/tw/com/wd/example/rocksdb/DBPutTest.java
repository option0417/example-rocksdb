package tw.com.wd.example.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;


public class DBPutTest {
    private String dbPath;
    private Options options;
    private RocksDB rocksDB;

    @Before
    public void beforeTest() {
        // Setup path of RocksDB
        this.dbPath = DBConfig.getDBRootPath() + File.separator + DBOpenTest.class.getSimpleName();

        // Setup option of RocksDB
        this.options = new Options();
        this.options.setCreateIfMissing(true);

        try {
            rocksDB = RocksDB.open(this.options, this.dbPath);
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
        String text = "just-for-test";

        try {
            this.rocksDB.put(text.getBytes(), text.getBytes());
            this.rocksDB.put(text.getBytes(), 5, text.length() - 5, text.getBytes(), 9, 4);
        } catch (Exception e) {
            e.printStackTrace();
            exception = e;
        }

        assertThat(exception, is(nullValue()));
        assertThat(this.rocksDB.get(text.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(text.getBytes())), is(text));
        assertThat(this.rocksDB.get("for-test".getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get("for-test".getBytes())), is("test"));
    }

    @Test
    public void testPutWithWriteOptions() throws RocksDBException {
        Exception exception = null;
        String text = "just-for-test";
        ByteBuffer testByteBuffer = ByteBuffer.allocateDirect("test-from-ByteBuffer".getBytes().length*2);
        testByteBuffer.put("test-from-ByteBuffer".getBytes());
        testByteBuffer.flip();

        try {
            WriteOptions writeOptions = new WriteOptions();

            this.rocksDB.put(writeOptions, text.getBytes(), text.getBytes());
            this.rocksDB.put(writeOptions, text.getBytes(), 5, text.length() - 5, text.getBytes(), 9, 4);
            this.rocksDB.put(writeOptions, testByteBuffer, testByteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
            exception = e;
        }

        assertThat(exception, is(nullValue()));
        assertThat(this.rocksDB.get(text.getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get(text.getBytes())), is(text));
        assertThat(this.rocksDB.get("for-test".getBytes()), is(notNullValue()));
        assertThat(new String(this.rocksDB.get("for-test".getBytes())), is("test"));
        assertThat(new String(this.rocksDB.get("test-from-ByteBuffer".getBytes())), is("test-from-ByteBuffer"));
    }
 }
