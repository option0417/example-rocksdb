package tw.com.wd.example.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;


public class PrimaryRocksDB implements Callable<Void> {
    private RocksDB rocksDB;
    private BlockingQueue<String> resultQueue;


    public PrimaryRocksDB(BlockingQueue<String> resultQueue) {
        super();
        this.resultQueue = resultQueue;

        String dbPath = DBConfig.getDBRootPath() + File.separator + "test";

        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            this.rocksDB = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Void call() throws Exception {
        String uuid = UUID
            .randomUUID().toString();
        //String putPrefix = "Data-" + uuid + "-";
        String putPrefix = "Data-";
        int count = 0;

        if (rocksDB != null) {
            while (true) {
                String putData = putPrefix + count;

                try {
                    rocksDB.put(putData.getBytes(), putData.getBytes());

                    this.resultQueue.put(putData);

                    System.out.printf("Put %s\n", putData);
                    count++;
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }

                Thread.sleep(500L);
            }
        }

        return null;
    }
}
