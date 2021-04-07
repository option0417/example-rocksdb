package tw.com.wd.example.rocksdb;

public class DBConfig {
    public static final String getDBRootPath() {
        return System.getProperty("core.db.path", "/tmp/rocksdb");
    }
}