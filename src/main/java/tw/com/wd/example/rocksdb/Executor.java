package tw.com.wd.example.rocksdb;


import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.UUID;


public class Executor {
    public static void main(String[] args) {
        System.out.printf("Hello RocksDB.\n");

        String uuid = UUID
            .randomUUID().toString();
        String putPrefix = "Data-" + uuid + "-";
        String dbPath = DBConfig.getDBRootPath() + File.separator + "test";

        Options options = new Options();
        options.setCreateIfMissing(true);

        RocksDB rocksDB1 = null;
        RocksDB rocksDB2 = null;
        try {
            rocksDB1 = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        try {
            rocksDB2 = RocksDB.openAsSecondary(options, dbPath, dbPath+"_secondary");
            rocksDB2.tryCatchUpWithPrimary();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        byte[] oldKeyBytes = null;
        if (rocksDB1 != null && rocksDB2 != null) {
            for (int count = 0; count < 100; count++) {
                String putData = putPrefix + count;

                try {
                    rocksDB1.put(putData.getBytes(), putData.getBytes());
                    rocksDB1.flush(new FlushOptions());
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (oldKeyBytes != null) {
                        byte[] value = rocksDB2.get(putData.getBytes());

                        if (value == null) {
                            System.out.printf("Not found\n");
                        } else {
                            System.out.printf("Found\n");
                        }
                    } else {
                        oldKeyBytes = putData.getBytes();
                    }
                } catch (RocksDBException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }
}
