package io.github.jiashunx.sdk.sqlite3.core.exception;

/**
 * SQLite3 SQL异常
 * @author jiashunx
 */
public class SQLite3SqlException extends SQLite3Exception {

    public SQLite3SqlException() {
        super();
    }

    public SQLite3SqlException(String message) {
        super(message);
    }

    public SQLite3SqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLite3SqlException(Throwable cause) {
        super(cause);
    }

}
