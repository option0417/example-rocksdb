package tw.com.wd.example.rocksdb;


public class Executor {
    public static void main(String[] args) {
        System.out.printf("Hello RocksDB.\n");



        ExDBOpen exDBOpen = new ExDBOpen(100);
        //exDBOpen.caseOpenOneTime();
        exDBOpen.caseOpenEveryTime();
    }
}
