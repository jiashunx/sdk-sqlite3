package io.github.jiashunx.sdk.sqlite3.core.function;

/**
 * 无入参无返回值Function
 * @author jiashunx
 */
public interface VoidFunc {

    /**
     * 无入参无返回值Function执行方法
     */
    void apply();

    /**
     * 无入参无返回值Function执行方法（捕获异常但不处理）
     */
    default void applyQuietly() {
        try {
            apply();
        } catch (Throwable throwable) {
            // do nothing.
        }
    }

}
