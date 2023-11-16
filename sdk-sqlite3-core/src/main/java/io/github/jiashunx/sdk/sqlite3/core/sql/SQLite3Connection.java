package io.github.jiashunx.sdk.sqlite3.core.sql;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.function.VoidFunc;
import io.github.jiashunx.sdk.sqlite3.core.pool.SQLite3ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * SQLite3 数据库连接
 * @author jiashunx
 */
public abstract class SQLite3Connection {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3Connection.class);

    /**
     * 默认返回值（空对象）
     */
    private static final byte[] DEFAULT_RETURN_VALUE = new byte[0];

    /**
     * SQLite3数据库连接池对象
     */
    private final SQLite3ConnectionPool connectionPool;

    /**
     * SQLite3数据库连接
     */
    private final Connection connection;

    /**
     * SQLite3数据库连接是否已关闭标志
     */
    private volatile boolean closed;

    /**
     * SQLite3数据库连接读写锁
     */
    private final ReentrantReadWriteLock actionLock = new ReentrantReadWriteLock();

    /**
     * SQLite3数据库连接名称
     */
    private String name;

    /**
     * SQLite3数据库连接构造方法
     * @param connectionPool 数据库连接池对象
     * @param connection 数据库连接对象
     */
    public SQLite3Connection(SQLite3ConnectionPool connectionPool, Connection connection) {
        this.connectionPool = Objects.requireNonNull(connectionPool);
        this.connection = Objects.requireNonNull(connection);
    }

    /**
     * 释放连接
     */
    public void release() {
        this.connectionPool.release(this);
    }

    /**
     * 数据库连接读处理（无返回值）
     * @param consumer 入参Connection对象Consumer实现
     * @throws SQLite3Exception SQLite3异常
     */
    public void read(Consumer<Connection> consumer) throws SQLite3Exception {
        read(c -> {
            consumer.accept(c);
            return DEFAULT_RETURN_VALUE;
        });
    }

    /**
     * 数据库连接读处理（有返回值）
     * @param function 入参Connection对象带返回值Function实现
     * @param <R> 泛型类型
     * @return 泛型类型对象
     * @throws SQLite3Exception SQLite3异常
     */
    public <R> R read(Function<Connection, R> function) throws SQLite3Exception {
        AtomicReference<R> reference = new AtomicReference<>();
        doCheck(() -> {
            connectionPool.getActionReadLock().lock();
            try {
                reference.set(function.apply(this.connection));
            } finally {
                connectionPool.getActionReadLock().unlock();;
            }
        });
        return reference.get();
    }

    /**
     * 数据库连接写处理（无返回值）
     * @param consumer 入参Connection对象Consumer实现
     * @throws SQLite3Exception SQLite3异常
     */
    public void write(Consumer<Connection> consumer) throws SQLite3Exception {
        write(c -> {
            consumer.accept(c);
            return DEFAULT_RETURN_VALUE;
        });
    }

    /**
     * 数据库连接写处理（有返回值）
     * @param function 入参Connection对象带返回值Function实现
     * @param <R> 泛型类型
     * @return 泛型类型对象
     * @throws SQLite3Exception SQLite3异常
     */
    public <R> R write(Function<Connection, R> function) throws SQLite3Exception {
        AtomicReference<R> reference = new AtomicReference<>();
        doCheck(() -> {
            connectionPool.getActionWriteLock().lock();
            try {
                reference.set(function.apply(connection));
            } finally {
                connectionPool.getActionWriteLock().unlock();
            }
        });
        return reference.get();
    }

    /**
     * 关闭连接
     * @throws SQLite3Exception SQLite3异常
     */
    public synchronized void close() throws SQLite3Exception {
        logger.debug("==>>关闭数据库连接");
        // 关闭连接需更新连接状态，因此进行写处理
        doWrite(() -> {
            if (closed) {
                return;
            }
            try {
                connection.close();
            } catch (Throwable throwable) {
                logger.error("connection [{}] close failed.", getName(), throwable);
            } finally {
                closed = true;
            }
        });
    }

    /**
     * 连接状态检查处理（检查是否已关闭）
     * @param voidFunc 无入参无返回值Function
     * @throws SQLite3Exception SQLite3异常
     */
    private void doCheck(VoidFunc voidFunc) throws SQLite3Exception {
        doRead(() -> {
            logger.debug("==>>数据库连接状态检查处理（检查是否已关闭）");
            if (closed) {
                throw new SQLite3Exception("connection is closed.");
            }
            if (voidFunc != null) {
                voidFunc.apply();
            }
        });
    }

    /**
     * 连接读处理
     * @param voidFunc 无入参无返回值Function
     */
    private void doRead(VoidFunc voidFunc) {
        actionLock.readLock().lock();
        try {
            if (voidFunc != null) {
                voidFunc.apply();
            }
        } finally {
            actionLock.readLock().unlock();
        }
    }

    /**
     * 连接写处理
     * @param voidFunc 无入参无返回值Function对象
     */
    private void doWrite(VoidFunc voidFunc) {
        actionLock.writeLock().lock();
        try {
            if (voidFunc != null) {
                voidFunc.apply();
            }
        } finally {
            actionLock.writeLock().unlock();
        }
    }

    /**
     * 设置SQLite3数据库连接名称
     * @param name SQLite3数据库连接名称
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取SQLite3数据库连接名称
     * @return SQLite3数据库连接名称
     */
    public String getName() {
        return name;
    }

}
