package tw.com.wd.example.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class SecondaryRocksDB implements Callable<Void> {
    private RocksDB rocksDB;
    private BlockingQueue<String> resultQueue;


    public SecondaryRocksDB(BlockingQueue<String> resultQueue) {
        super();
        this.resultQueue = resultQueue;

        String dbPath = DBConfig.getDBRootPath() + File.separator + "test";

        Options options = new Options();
        options.setMaxOpenFiles(-1);
        options.setCreateIfMissing(false);

        try {
            this.rocksDB = RocksDB.openAsSecondary(options, dbPath, dbPath+"_secondary");
            this.rocksDB.tryCatchUpWithPrimary();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Void call() throws Exception {
        int count = 0;

        if (rocksDB != null) {
            while (true) {
                String putData = this.resultQueue.poll(500L, TimeUnit.MILLISECONDS);

                if (putData != null) {
                    System.out.printf("Fetch key: %s", putData);

                    try {
                        byte[] value = rocksDB.get(putData.getBytes());

                        if (value != null) {
                            System.out.printf("\t\tFound value: %s\n", new String(value));
                            count++;
                        } else {
                            System.out.printf("\t\tNot found with %s\n", putData);
                            rocksDB.tryCatchUpWithPrimary();
                        }
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                    Thread.sleep(500L);
                }
            }
        }

        return null;
    }
}
