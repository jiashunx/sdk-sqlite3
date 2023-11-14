package io.github.jiashunx.sdk.sqlite3.core.type;

/**
 * SQLite3 连接池状态枚举
 * @author jiashunx
 */
public enum  SQLite3ConnectionPoolStatus {

    /**
     * 运行中
     */
    RUNNING,

    /**
     * 关闭中
     */
    CLOSING,

    /**
     * 已关闭
     */
    SHUTDOWN;

}
