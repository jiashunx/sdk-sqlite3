package io.github.jiashunx.sdk.sqlite3.core.exception;

/**
 * SQLite3 异常基类
 * @author jiashunx
 */
public class SQLite3Exception extends RuntimeException {

    public SQLite3Exception() {
        super();
    }

    public SQLite3Exception(String message) {
        super(message);
    }

    public SQLite3Exception(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLite3Exception(Throwable cause) {
        super(cause);
    }

}
