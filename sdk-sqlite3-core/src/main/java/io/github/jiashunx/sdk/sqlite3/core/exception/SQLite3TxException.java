package io.github.jiashunx.sdk.sqlite3.core.exception;

/**
 * SQLite3 事务异常
 * @author jiashunx
 */
public class SQLite3TxException extends SQLite3Exception {

    public SQLite3TxException() {
        super();
    }

    public SQLite3TxException(String message) {
        super(message);
    }

    public SQLite3TxException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLite3TxException(Throwable cause) {
        super(cause);
    }

}
