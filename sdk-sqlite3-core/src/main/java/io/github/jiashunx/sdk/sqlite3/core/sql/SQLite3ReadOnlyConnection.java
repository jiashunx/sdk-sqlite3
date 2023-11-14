package io.github.jiashunx.sdk.sqlite3.core.sql;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.pool.SQLite3ConnectionPool;

import java.sql.Connection;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * SQLite3 读连接（仅读）
 * @author jiashunx
 */
public class SQLite3ReadOnlyConnection extends SQLite3Connection {

    public SQLite3ReadOnlyConnection(SQLite3ConnectionPool connectionPool, Connection connection) {
        super(connectionPool, connection);
    }

    @Override
    public void write(Consumer<Connection> consumer) throws SQLite3Exception, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R write(Function<Connection, R> function) throws SQLite3Exception, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

}
