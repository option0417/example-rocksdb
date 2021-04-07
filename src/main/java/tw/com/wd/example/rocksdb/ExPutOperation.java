package tw.com.wd.example.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;


public class ExPutOperation {
    private RocksDB rocksDB;


    public ExPutOperation() {
        super();
        try {
            this.rocksDB = RocksDB.open(DBConfig.getDBRootPath());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (this.rocksDB != null) {
            this.rocksDB.close();
        }
    }
}
