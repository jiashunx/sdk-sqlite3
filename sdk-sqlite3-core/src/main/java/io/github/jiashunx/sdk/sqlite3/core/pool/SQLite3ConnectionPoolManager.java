package io.github.jiashunx.sdk.sqlite3.core.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

/**
 * SQLite3数据库连接池管理器
 * @author jiashunx
 */
public class SQLite3ConnectionPoolManager {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3ConnectionPoolManager.class);

    /**
     * 全局数据库连接池持有map
     */
    private static final Map<String, SQLite3ConnectionPool> POOL_MAP  = new HashMap<>();

    /**
     * 连接池默认连接数量（写连接*1，读连接*15）
     */
    public static final int DEFAULT_POOL_SIZE = 16;

    /**
     * 连接池最大连接数量（写连接*1，读连接*255）
     */
    public static final int MAX_POOL_SIZE = 256;

    /**
     * 连接池最小连接数量（写连接*1，读连接*1）
     */
    public static final int MIN_POOL_SIZE = 2;

    /**
     * 默认数据库用户名
     */
    public static final String DEFAULT_USERNAME = "sqlite";

    /**
     * 默认数据库密码
     */
    public static final String DEFAULT_PASSWORD = "sqlite";

    /**
     * 私有构造方法
     */
    private SQLite3ConnectionPoolManager() {}

    /**
     * 创建数据库连接池（默认连接池连接数量16，默认数据库用户名密码：sqlite/sqlite）
     * @param fileName 数据库文件名
     * @return 数据库连接池对象
     */
    public static SQLite3ConnectionPool create(String fileName) {
        return create(fileName, DEFAULT_POOL_SIZE);
    }

    /**
     * 创建数据库连接池（默认数据库用户名密码：sqlite/sqlite）
     * @param fileName 数据库文件名
     * @param poolSize 连接池连接数量
     * @return 数据库连接池对象
     */
    public static SQLite3ConnectionPool create(String fileName, int poolSize) {
        return create(fileName, poolSize, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    /**
     * 创建数据库连接池（默认连接池连接数量16）
     * @param fileName 数据库文件名
     * @param username 数据库用户名
     * @param password 数据库密码
     * @return 数据库连接池对象
     */
    public static SQLite3ConnectionPool create(String fileName, String username, String password) {
        return create(fileName, DEFAULT_POOL_SIZE, username, password);
    }

    /**
     * 创建数据库连接池
     * @param fileName 数据库文件名
     * @param poolSize 连接池连接数量
     * @param username 数据库用户名
     * @param password 数据库密码
     * @return 数据库连接池对象
     */
    public synchronized static SQLite3ConnectionPool create(String fileName, int poolSize, String username, String password) {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("sqlite db filename can not be null or empty");
        }
        if (poolSize > MAX_POOL_SIZE || poolSize < MIN_POOL_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "sqlite db pool size can not less than %d and not large than %d"
                    , MIN_POOL_SIZE, MAX_POOL_SIZE));
        }
        try {
            File dbFile = new File(fileName);
            String dbFilePath = dbFile.getAbsolutePath().replace("\\", "/");
            File dbFileDir = dbFile.getParentFile();
            if (!dbFileDir.exists()) {
                dbFileDir.mkdirs();
            }
            String $url = "jdbc:sqlite:" + dbFilePath;
            String $username = String.valueOf(username);
            String $password = String.valueOf(password);
            SQLite3ConnectionPool pool = POOL_MAP.get(dbFilePath);
            if (pool != null) {
                if (poolSize > pool.getReadConnectionPoolSize()) {
                    logger.info("found exists sqlite connection pool: {}", pool.getPoolName());
                    logger.info("create sqlite connection, url: {}, username: {}, password: {}", $url, $username, $password);
                    for (int i = 0, size = poolSize - pool.getReadConnectionPoolSize(); i < size; i++) {
                        pool.addReadConnection(DriverManager.getConnection($url, $username, $password));
                    }
                }
                return pool;
            }
            logger.info("create sqlite connection, url: {}, username: {}, password: {}", $url, $username, $password);
            // 写连接=1
            Connection writeConnection = DriverManager.getConnection($url, $username, $password);
            // 读连接=N-1
            Connection[] readConnectionArr = new Connection[poolSize - 1];
            for (int i = 0 ; i < poolSize - 1; i++) {
                readConnectionArr[i] = DriverManager.getConnection($url, $username, $password);;
            }
            pool = new SQLite3ConnectionPool(writeConnection, readConnectionArr);
            POOL_MAP.put(dbFilePath, pool);
            return pool;
        } catch (Throwable throwable) {
            logger.error("create sqlite connection pool failed.", throwable);
        }
        return null;
    }

}
