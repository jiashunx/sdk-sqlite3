package io.github.jiashunx.sdk.sqlite3.core.sql;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.pool.SQLite3ConnectionPool;

import java.sql.Connection;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * SQLite3 写连接（支持写+读）
 * @author jiashunx
 */
public class SQLite3WriteOnlyConnection extends SQLite3Connection {

    public SQLite3WriteOnlyConnection(SQLite3ConnectionPool connectionPool, Connection connection) {
        super(connectionPool, connection);
    }

    @Override
    public void read(Consumer<Connection> consumer) throws SQLite3Exception {
        super.write(consumer);
    }

    @Override
    public <R> R read(Function<Connection, R> function) throws SQLite3Exception {
        return super.write(function);
    }

}
