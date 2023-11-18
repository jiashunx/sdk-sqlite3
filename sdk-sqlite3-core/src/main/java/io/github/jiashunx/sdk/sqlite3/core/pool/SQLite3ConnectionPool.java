package io.github.jiashunx.sdk.sqlite3.core.pool;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.function.VoidFunc;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3Connection;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3ReadOnlyConnection;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3WriteOnlyConnection;
import io.github.jiashunx.sdk.sqlite3.core.type.SQLite3ConnectionPoolStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SQLite3 连接池（包含读连接池及写连接池，读连接池连接数量=1，写连接池连接数量>=1）
 * @author jiashunx
 */
public class SQLite3ConnectionPool {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3ConnectionPool.class);

    /**
     * 连接池计数器
     */
    private static final AtomicInteger counter = new AtomicInteger(0);

    /**
     * 连接池读写锁
     */
    private final ReentrantReadWriteLock actionLock = new ReentrantReadWriteLock();

    /**
     * 连接池名称
     */
    private final String poolName;

    /**
     * 写连接池
     */
    private final LinkedList<SQLite3Connection> writeConnectionPool = new LinkedList<>();

    /**
     * 写连接池持有连接总数量
     */
    private final int writeConnectionPoolSize;

    /**
     * 写连接池状态
     */
    private volatile SQLite3ConnectionPoolStatus writeConnectionPoolStatus;

    /**
     * 读连接池
     */
    private final LinkedList<SQLite3Connection> readConnectionPool = new LinkedList<>();

    /**
     * 读连接池持有连接总数量
     */
    private int readConnectionPoolSize;

    /**
     * 读连接池状态
     */
    private volatile SQLite3ConnectionPoolStatus readConnectionPoolStatus;

    /**
     * SQLite3数据库连接池构造方法
     * @param writeConn 数据库写连接对象（不可为null）
     * @param readConnArr 数据库读连接对象数字（可变参数，至少传入一个）
     */
    public SQLite3ConnectionPool(Connection writeConn, Connection... readConnArr) {
        if (writeConn == null) {
            throw new IllegalArgumentException("write-only connection can't be null");
        }
        if (readConnArr.length == 0) {
            throw new IllegalArgumentException("there is no read-only connections");
        }
        for (Connection readConn : readConnArr) {
            if (readConn == null) {
                throw new IllegalArgumentException("read-only connection can't be null");
            }
        }
        this.poolName = "sqlite3-pool-" + counter.incrementAndGet();
        SQLite3Connection writeConnection = new SQLite3WriteOnlyConnection(this, writeConn);
        writeConnection.setName(this.poolName + "-write-1");
        this.writeConnectionPool.add(writeConnection);
        this.writeConnectionPoolSize = this.writeConnectionPool.size();
        this.writeConnectionPoolStatus = SQLite3ConnectionPoolStatus.RUNNING;
        for (int index = 0; index < readConnArr.length; index++) {
            Connection readConn = readConnArr[index];
            SQLite3ReadOnlyConnection readConnection = new SQLite3ReadOnlyConnection(this, readConn);
            readConnection.setName(this.poolName + "-read-" + (index + 1));
            this.readConnectionPool.add(readConnection);
        }
        this.readConnectionPoolSize = this.readConnectionPool.size();
        this.readConnectionPoolStatus = SQLite3ConnectionPoolStatus.RUNNING;
    }

    /**
     * 添加数据库读连接
     * @param connection 数据库连接对象
     * @throws SQLite3Exception SQLite3异常
     */
    public synchronized void addReadConnection(Connection connection) throws SQLite3Exception {
        if (connection != null) {
            logger.debug("==>>添加数据库读连接: {}", connection.hashCode());
            checkReadConnectionPoolStatus();
            SQLite3ReadOnlyConnection readConnection = new SQLite3ReadOnlyConnection(this, connection);
            synchronized (readConnectionPool) {
                readConnection.setName(getPoolName() + "-read-" + readConnectionPoolSize+ 1);
                readConnectionPool.addLast(readConnection);
                readConnectionPoolSize++;
                readConnectionPool.notifyAll();
            }
        }
    }

    /**
     * 释放数据库连接（写连接/读连接）
     * @param connection 数据库连接对象（写连接对象/读连接对象）
     */
    public void release(SQLite3Connection connection) {
        if (connection instanceof SQLite3ReadOnlyConnection) {
            logger.debug("==>>释放数据库连接（读连接）");
            release(readConnectionPool, connection);
            return;
        }
        if (connection instanceof SQLite3WriteOnlyConnection) {
            logger.debug("==>>释放数据库连接（写连接）");
            release(writeConnectionPool, connection);
        }
    }

    /**
     * 释放数据库连接（写连接/读连接）
     * @param pool 数据库连接池（写连接池/读连接池）
     * @param connection 数据库连接对象（写连接对象/读连接对象）
     */
    private void release(LinkedList<SQLite3Connection> pool, SQLite3Connection connection) {
        if (pool != null && connection != null) {
            synchronized (pool) {
                // 连接释放后通知消费者连接池已归还连接
                if (!pool.contains(connection)) {
                    pool.addLast(connection);
                }
                pool.notifyAll();
            }
        }
    }

    /**
     * 关闭SQLite3数据库连接池（写连接池及读连接池均关闭）
     * @throws InterruptedException
     * @throws SQLite3Exception
     */
    public synchronized void close() throws InterruptedException, SQLite3Exception {
        logger.debug("==>>关闭SQLite3数据库连接池");
        synchronized (writeConnectionPool) {
            logger.debug("==>>关闭SQLite3数据库连接池（写连接池）");
            writeConnectionPoolStatus = SQLite3ConnectionPoolStatus.CLOSING;
            while (writeConnectionPool.size() != writeConnectionPoolSize) {
                writeConnectionPool.wait();
            }
            for (SQLite3Connection connection: writeConnectionPool) {
                connection.close();
            }
            writeConnectionPoolStatus = SQLite3ConnectionPoolStatus.SHUTDOWN;
        }
        synchronized (readConnectionPool) {
            logger.debug("==>>关闭SQLite3数据库连接池（读连接池）");
            readConnectionPoolStatus = SQLite3ConnectionPoolStatus.CLOSING;
            while (readConnectionPool.size() != readConnectionPoolSize) {
                readConnectionPool.wait();
            }
            for (SQLite3Connection connection: readConnectionPool) {
                connection.close();
            }
            readConnectionPoolStatus = SQLite3ConnectionPoolStatus.SHUTDOWN;
        }
    }

    /**
     * 获取SQLite3 数据库写连接
     * @return SQLite3 数据库写连接对象
     * @throws SQLite3Exception SQLite3异常
     */
    public SQLite3Connection fetchWriteConnection() throws SQLite3Exception {
        try {
            return fetchWriteConnection(0);
        } catch (InterruptedException exception) {
            logger.error("fetch write connection from pool [{}] failed, there is InterruptedException occurred", getPoolName(), exception);
        }
        return null;
    }

    /**
     * 获取SQLite3 数据库写连接
     * @param timeoutMillis 获取连接超时时间（毫秒），小于等于零则表示无超时时间
     * @return SQLite3 数据库写连接对象
     * @throws InterruptedException 中断异常
     * @throws SQLite3Exception SQLite3异常
     */
    public SQLite3Connection fetchWriteConnection(long timeoutMillis) throws InterruptedException, SQLite3Exception {
        logger.debug("==>>获取SQLite3 数据库写连接，超时时间：{}ms", timeoutMillis);
        return fetchConnection(writeConnectionPool, timeoutMillis, this::checkWriteConnectionPoolStatus);
    }

    /**
     * 获取SQLite3 数据库读连接
     * @return SQLite3 数据库读连接对象
     * @throws SQLite3Exception SQLite3异常
     */
    public SQLite3Connection fetchReadConnection() throws SQLite3Exception {
        try {
            return fetchReadConnection(0);
        } catch (InterruptedException exception) {
            logger.error("fetch read connection from pool [{}] failed, there is InterruptedException occurred", getPoolName(), exception);
        }
        return null;
    }

    /**
     * 获取SQLite3 数据库读连接
     * @param timeoutMillis 获取连接超时时间（毫秒），小于等于零则表示无超时时间
     * @return SQLite3 数据库读连接对象
     * @throws InterruptedException 中断异常
     * @throws SQLite3Exception SQLite3异常
     */
    public SQLite3Connection fetchReadConnection(long timeoutMillis) throws InterruptedException, SQLite3Exception {
        logger.debug("==>>获取SQLite3 数据库读连接，超时时间：{}ms", timeoutMillis);
        return fetchConnection(readConnectionPool, timeoutMillis, this::checkReadConnectionPoolStatus);
    }

    /**
     * 获取SQLite3 数据库连接
     * @param pool 连接池对象（读连接池/写连接池）
     * @param timeoutMillis 获取连接超时时间（毫秒），小于等于零则表示无超时时间
     * @param statusVerifier 连接池状态检查
     * @return SQLite3 数据库连接对象
     * @throws InterruptedException 中断异常
     * @throws SQLite3Exception SQLite3异常
     */
    private SQLite3Connection fetchConnection(LinkedList<SQLite3Connection> pool, long timeoutMillis, VoidFunc statusVerifier)
            throws InterruptedException, SQLite3Exception {
        // 1. 连接池对象加锁
        synchronized (pool) {
            // 2.1 超时时间小于0，则表示无超时时间，一直阻塞等待可用连接
            if (timeoutMillis <= 0) {
                while (pool.isEmpty()) {
                    // 2.1.1 连接池有可用连接前一直等待
                    pool.wait();
                }
                // 2.1.2 获取连接前进行状态检查
                statusVerifier.apply();
                // 2.1.3 获取连接
                return pool.removeFirst();
            } else {
                // 2.2 有超时时间，等待固定时间
                long future = System.currentTimeMillis() + timeoutMillis;
                long remaining = timeoutMillis;
                while (pool.isEmpty() && remaining > 0) {
                    // 2.2.1 连接池有可用连接前等待固定时间
                    pool.wait(remaining);
                    remaining = future - System.currentTimeMillis();
                }
                // 2.2.2 获取连接前进行状态检查
                statusVerifier.apply();
                // 2.2.3 获取连接（已过超时时间连接池可能仍无连接）
                SQLite3Connection connection = null;
                if (!pool.isEmpty()) {
                    connection = pool.removeFirst();
                }
                return connection;
            }
        }
    }

    /**
     * 读连接池状态检查（检查是否运行中）
     * @throws SQLite3Exception 不为running状态则抛出异常
     */
    private void checkReadConnectionPoolStatus() throws SQLite3Exception {
        if (readConnectionPoolStatus == SQLite3ConnectionPoolStatus.CLOSING) {
            throw new SQLite3Exception(String.format("connection pool [%s] for reading is closing.", getPoolName()));
        }
        if (readConnectionPoolStatus == SQLite3ConnectionPoolStatus.SHUTDOWN) {
            throw new SQLite3Exception(String.format("connection pool [%s] for reading is closed.", getPoolName()));
        }
        if (readConnectionPoolStatus == SQLite3ConnectionPoolStatus.RUNNING) {
            return;
        }
        throw new SQLite3Exception(String.format("connection pool [%s] for reading has illegal status: %s", getPoolName(), readConnectionPoolStatus));
    }

    /**
     * 写连接池状态检查（检查是否运行中）
     * @throws SQLite3Exception 不为running状态则抛出异常
     */
    private void checkWriteConnectionPoolStatus() throws SQLite3Exception {
        if (writeConnectionPoolStatus == SQLite3ConnectionPoolStatus.CLOSING) {
            throw new SQLite3Exception(String.format("connection pool [%s] for writing is closing.", getPoolName()));
        }
        if (writeConnectionPoolStatus == SQLite3ConnectionPoolStatus.SHUTDOWN) {
            throw new SQLite3Exception(String.format("connection pool [%s] for writing is closed.", getPoolName()));
        }
        if (writeConnectionPoolStatus == SQLite3ConnectionPoolStatus.RUNNING) {
            return;
        }
        throw new SQLite3Exception(String.format("connection pool [%s] for writing has illegal status: %s", getPoolName(), writeConnectionPoolStatus));
    }

    /**
     * 获取连接池名称
     * @return 连接池名称
     */
    public String getPoolName() {
        return poolName;
    }

    /**
     * 获取连接池读写锁
     * @return 连接池读写锁
     */
    public ReentrantReadWriteLock getActionLock() {
        return actionLock;
    }

    /**
     * 获取连接池读锁
     * @return 连接池读锁
     */
    public ReentrantReadWriteLock.ReadLock getActionReadLock() {
        return getActionLock().readLock();
    }

    /**
     * 获取连接池写锁
     * @return 连接池写锁
     */
    public ReentrantReadWriteLock.WriteLock getActionWriteLock() {
        return getActionLock().writeLock();
    }

    /**
     * 获取读连接池持有连接总数量
     * @return 读连接池持有连接总数量
     */
    public int getReadConnectionPoolSize() {
        return readConnectionPoolSize;
    }
}
