package tw.com.wd.example.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.UUID;


public class ExDBOpen {
    private int putCount;


    public ExDBOpen(int TestRound) {
        super();
        this.putCount = TestRound;
    }

    public void caseOpenOneTime() {
        // Setup the key and value
        String uuid = UUID
            .randomUUID().toString();
        String putPrefix = "Data-" + uuid + "-";

        // Setup path of RocksDB
        String dbPath = DBConfig.getDBRootPath() + File.separator + ExDBOpen.class.getSimpleName();

        long startTime = System.currentTimeMillis();
        try (RocksDB rocksDB = RocksDB.open(dbPath)) {
            for (int count = 0; count < putCount; count++) {
                String putData = putPrefix + count;
                rocksDB.put(putData.getBytes(), putData.getBytes());
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();

        System.out.printf("Cost: %d ms\n", endTime - startTime);
        try {
            RocksDB.destroyDB(dbPath, new Options());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void caseOpenEveryTime() {
        // Setup the key and value
        String uuid = UUID
            .randomUUID().toString();
        String putPrefix = "Data-" + uuid + "-";

        // Setup path of RocksDB
        String dbPath = DBConfig.getDBRootPath() + File.separator + ExDBOpen.class.getSimpleName();

        Options options = new Options();
        options.setTargetFileSizeBase(1024000);
        options.setCreateIfMissing(true);


        long startTime = System.currentTimeMillis();
        for (int count = 0; count < putCount; count++) {
            String putData = putPrefix + count;

            try (RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                rocksDB.put(putData.getBytes(), putData.getBytes());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();

        System.out.printf("Cost: %d ms\n", endTime - startTime);


    }
}
