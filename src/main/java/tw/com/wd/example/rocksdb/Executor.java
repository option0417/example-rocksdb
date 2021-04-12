package tw.com.wd.example.rocksdb;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;


public class Executor {
    public static void main(String[] args) {
        System.out.printf("Hello RocksDB.\n");

        LinkedBlockingQueue<String> resultQueue = new LinkedBlockingQueue<String>();

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(new PrimaryRocksDB(resultQueue));

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executorService.submit(new SecondaryRocksDB(resultQueue));

        System.out.printf("Main thread done\n");
    }
}
