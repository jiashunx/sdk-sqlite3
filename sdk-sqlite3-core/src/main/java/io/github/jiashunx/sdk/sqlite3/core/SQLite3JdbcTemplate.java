package io.github.jiashunx.sdk.sqlite3.core;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.function.VoidFunc;
import io.github.jiashunx.sdk.sqlite3.core.pool.SQLite3ConnectionPool;
import io.github.jiashunx.sdk.sqlite3.core.pool.SQLite3ConnectionPoolManager;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3Connection;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3PreparedStatement;
import io.github.jiashunx.sdk.sqlite3.core.util.SQLite3Utils;
import io.github.jiashunx.sdk.sqlite3.metadata.ColumnMetadata;
import io.github.jiashunx.sdk.sqlite3.metadata.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 封装SQLite3 JDBC操作模型
 * @author jiashunx
 */
public class SQLite3JdbcTemplate {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3JdbcTemplate.class);

    /**
     * 空对象实现
     */
    private static final Object EMPTY_OBJECT = new byte[0];

    /**
     * 上下文-事务标志
     */
    private static final ThreadLocal<Boolean> TX_MODE = new ThreadLocal<>();

    /**
     * 上下文-数据库连接
     */
    private static final ThreadLocal<SQLite3Connection> TX_CONNECTION = new ThreadLocal<>();

    /**
     * 从上下文获取当前是否事务模式
     * @return 上下文-事务标志
     */
    private static boolean isInTxMode() {
        boolean isInTxMode = TX_MODE.get() != null && TX_MODE.get();
        logger.debug("==>>从上下文获取当前是否事务模式: {}", isInTxMode);
        return isInTxMode;
    }

    /**
     * 从上下文获取当前数据库连接
     * @return 上下文-数据库连接
     */
    private static SQLite3Connection fetchTxConnection() {
        logger.debug("==>>从上下文获取当前数据库连接");
        return TX_CONNECTION.get();
    }

    /**
     * 设置上下文数据库连接
     * @param connection 数据库连接
     */
    private static void setTxMode(SQLite3Connection connection) {
        logger.debug("==>>设置上下文数据库连接");
        if (isInTxMode() && fetchTxConnection() != connection) {
            throw new SQLite3Exception("transaction connection conflict.");
        }
        TX_CONNECTION.set(Objects.requireNonNull(connection));
        TX_MODE.set(true);
    }

    /**
     * 重置上下文事务模式及数据库连接
     */
    private static void resetTxMode() {
        logger.debug("==>>重置上下文事务模式及数据库连接");
        TX_CONNECTION.remove();
        TX_MODE.remove();
    }

    /**
     * SQLite3数据库连接池
     */
    private SQLite3ConnectionPool connectionPool;

    /**
     * 构造方法
     * @param fileName SQLite3数据库地址
     */
    public SQLite3JdbcTemplate(String fileName) {
        this(SQLite3ConnectionPoolManager.create(fileName));
    }

    /**
     * 构造方法
     * @param pool SQLite3数据库连接池
     */
    public SQLite3JdbcTemplate(SQLite3ConnectionPool pool) {
        this();
        this.connectionPool = Objects.requireNonNull(pool);
    }

    /**
     * 构造方法（私有）
     */
    private SQLite3JdbcTemplate() {}

    /**
     * 获取SQLite3写连接
     * @return SQLite3写连接
     */
    private SQLite3Connection fetchWriteConnection() {
        if (isInTxMode()) {
            return fetchTxConnection();
        }
        return connectionPool.fetchWriteConnection();
    }

    /**
     * 写处理
     * @param consumer 写处理消费
     */
    private void write(Consumer<Connection> consumer) {
        write(fetchWriteConnection(), consumer);
    }

    /**
     * 写处理
     * @param connection SQLite3写连接
     * @param consumer 写处理消费
     */
    private void write(SQLite3Connection connection, Consumer<Connection> consumer) {
        logger.debug("==>>写处理");
        connection.write(c -> {
            try {
                consumer.accept(c);
            } finally {
                if (!isInTxMode()) {
                    connection.release();
                }
            }
        });
    }

    /**
     * 写处理
     * @param function 写处理方法
     * @param <R> 写处理返回对象类型
     * @return 写处理返回对象
     */
    private <R> R write(Function<Connection, R> function) {
        return write(fetchWriteConnection(), function);
    }

    /**
     * 写处理
     * @param connection SQLite3写连接
     * @param function 写处理方法
     * @param <R> 写处理返回对象类型
     * @return 写处理返回对象
     */
    private <R> R write(SQLite3Connection connection, Function<Connection, R> function) {
        logger.debug("==>>写处理");
        return connection.write(c -> {
            try {
                return function.apply(c);
            } finally {
                if (!isInTxMode()) {
                    connection.release();
                }
            }
        });
    }

    /**
     * 获取SQLite3读连接
     * @return SQLite3读连接
     */
    private SQLite3Connection fetchReadConnection() {
        if (isInTxMode()) {
            return fetchTxConnection();
        }
        return connectionPool.fetchReadConnection();
    }

    /**
     * 读处理
     * @param consumer 读处理消费
     */
    private void read(Consumer<Connection> consumer) {
        read(fetchReadConnection(), consumer);
    }

    /**
     * 读处理
     * @param connection SQLite3读连接
     * @param consumer 读处理消费
     */
    private void read(SQLite3Connection connection, Consumer<Connection> consumer) {
        logger.debug("==>>读处理");
        connection.read(c -> {
            try {
                consumer.accept(c);
            } finally {
                if (!isInTxMode()) {
                    connection.release();
                }
            }
        });
    }

    /**
     * 读处理
     * @param function 读处理方法
     * @param <R> 读处理返回对象类型
     * @return 读处理返回对象
     */
    private <R> R read(Function<Connection, R> function) {
        return read(fetchReadConnection(), function);
    }

    /**
     * 读处理
     * @param connection SQLite3读连接
     * @param function 读处理方法
     * @param <R> 读处理返回对象类型
     * @return 读处理返回对象
     */
    private <R> R read(SQLite3Connection connection, Function<Connection, R> function) {
        logger.debug("==>>读处理");
        return connection.read(c -> {
            try {
                return function.apply(c);
            } finally {
                if (!isInTxMode()) {
                    connection.release();
                }
            }
        });
    }

//    public <R> R queryForObj(String sql, Class<R> klass) throws SQLite3Exception {
//        return queryForObj(sql, statement -> {}, klass);
//    }

//    public <R> R queryForObj(String sql, Consumer<SQLite3PreparedStatement> consumer, Class<R> klass)
//            throws SQLite3Exception {
//        List<R> retList = queryForList(sql, consumer, klass);
//        if (retList == null || retList.isEmpty()) {
//            return null;
//        }
//        if (retList.size() > 1) {
//            throw new SQLite3Exception(String.format("query result contains more than one column, sql: %s", sql));
//        }
//        return retList.get(0);
//    }

//    public <R> List<R> queryForList(String sql, Class<R> klass) throws SQLite3Exception {
//        return queryForList(sql, statement -> {}, klass);
//    }

//    public <R> List<R> queryForList(String sql, Consumer<SQLite3PreparedStatement> consumer, Class<R> klass)
//            throws SQLite3Exception {
//        return SQLite3Utils.parseQueryResult(queryForResult(sql, consumer), klass);
//    }

    /**
     * 查询并返回boolean值
     * @param sql 待执行sql语句
     * @return boolean值
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean queryForBoolean(String sql) throws SQLite3Exception {
        return queryForBoolean(sql, s -> {});
    }

    /**
     * 查询并返回boolean值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return boolean值
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean queryForBoolean(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Boolean.parseBoolean(queryForString(sql, consumer));
    }

    /**
     * 查询并返回byte值
     * @param sql 待执行sql语句
     * @return byte值
     * @throws SQLite3Exception SQLite3Exception
     */
    public byte queryForByte(String sql) throws SQLite3Exception {
        return queryForByte(sql, s -> {});
    }

    /**
     * 查询并返回byte值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return byte值
     * @throws SQLite3Exception SQLite3Exception
     */
    public byte queryForByte(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Byte.parseByte(queryForString(sql, consumer));
    }

    /**
     * 查询并返回short值
     * @param sql 待执行sql语句
     * @return short值
     * @throws SQLite3Exception SQLite3Exception
     */
    public short queryForShort(String sql) throws SQLite3Exception {
        return queryForShort(sql, s -> {});
    }

    /**
     * 查询并返回short值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return short值
     * @throws SQLite3Exception SQLite3Exception
     */
    public short queryForShort(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Short.parseShort(queryForString(sql, consumer));
    }

    /**
     * 查询并返回int值
     * @param sql 待执行sql语句
     * @return int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int queryForInt(String sql) throws SQLite3Exception {
        return queryForInt(sql, s -> {});
    }

    /**
     * 查询并返回int值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int queryForInt(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Integer.parseInt(queryForString(sql, consumer));
    }

    /**
     * 查询并返回float值
     * @param sql 待执行sql语句
     * @return float值
     * @throws SQLite3Exception SQLite3Exception
     */
    public float queryForFloat(String sql) throws SQLite3Exception {
        return queryForFloat(sql, s -> {});
    }

    /**
     * 查询并返回float值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return float值
     * @throws SQLite3Exception SQLite3Exception
     */
    public float queryForFloat(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Float.parseFloat(queryForString(sql, consumer));
    }

    /**
     * 查询并返回double值
     * @param sql 待执行sql语句
     * @return double值
     * @throws SQLite3Exception SQLite3Exception
     */
    public double queryForDouble(String sql) throws SQLite3Exception {
        return queryForDouble(sql, s -> {});
    }

    /**
     * 查询并返回double值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return double值
     * @throws SQLite3Exception SQLite3Exception
     */
    public double queryForDouble(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Double.parseDouble(queryForString(sql, consumer));
    }

    /**
     * 查询并返回String值
     * @param sql 待执行sql语句
     * @return String值
     * @throws SQLite3Exception SQLite3Exception
     */
    public String queryForString(String sql) throws SQLite3Exception {
        return queryForString(sql, s -> {});
    }

    /**
     * 查询并返回String值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return String值
     * @throws SQLite3Exception SQLite3Exception
     */
    public String queryForString(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return queryForOneValue(sql, consumer).toString();
    }

    /**
     * 查询并返回单个字段值
     * @param sql 待执行sql语句
     * @return 单个字段值
     * @throws SQLite3Exception SQLite3Exception
     */
    public Object queryForOneValue(String sql) throws SQLite3Exception {
        return queryForOneValue(sql, s -> {});
    }

    /**
     * 查询并返回单个字段值
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return 单个字段值
     * @throws SQLite3Exception SQLite3Exception
     */
    public Object queryForOneValue(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        Map<String, Object> resultMap = queryForMap(sql, consumer);
        if (resultMap == null || resultMap.isEmpty()) {
            throw new SQLite3Exception(String.format("query result is null, sql: %s", sql));
        }
        if (resultMap.size() > 1) {
            throw new SQLite3Exception(String.format("query result contains more than one column, sql: %s", sql));
        }
        return resultMap.get(resultMap.keySet().toArray(new String[0])[0]);
    }

    /**
     * 查询并返回单条数据Map
     * @param sql 待执行sql语句
     * @return 单条数据Map
     * @throws SQLite3Exception SQLite3Exception
     */
    public Map<String, Object> queryForMap(String sql) throws SQLite3Exception {
        return queryForMap(sql, s -> {});
    }

    /**
     * 查询并返回单条数据Map
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return 单条数据Map
     * @throws SQLite3Exception SQLite3Exception
     */
    public Map<String, Object> queryForMap(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        List<Map<String, Object>> mapList = queryForList(sql, consumer);
        Map<String, Object> retMap = null;
        if (mapList != null && !mapList.isEmpty()) {
            if (mapList.size() == 1) {
                return mapList.get(0);
            }
            throw new SQLite3Exception(String.format("query result contains more than one row, sql: %s", sql));
        }
        return retMap;
    }

    /**
     * 查询并返回数据集List
     * @param sql 待执行sql语句
     * @return 数据集List
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Map<String, Object>> queryForList(String sql) throws SQLite3Exception {
        return queryForList(sql, s -> {});
    }

    /**
     * 查询并返回数据集List
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return 数据集List
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Map<String, Object>> queryForList(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return queryForResult(sql, consumer).getRetMapList();
    }

    /**
     * 查询并返回查询结果
     * @param sql 待执行sql语句
     * @return 查询结果
     * @throws SQLite3Exception SQLite3Exception
     */
    public QueryResult queryForResult(String sql) throws SQLite3Exception {
        return queryForResult(sql, s -> {});
    }

    /**
     * 查询并返回查询结果
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return 查询结果
     * @throws SQLite3Exception SQLite3Exception
     */
    public QueryResult queryForResult(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return read(connection -> {
            SQLite3PreparedStatement statement = null;
            ResultSet resultSet = null;
            try {
                logger.debug("==>>查询并返回查询结果，执行sql：{}", sql);
                statement = new SQLite3PreparedStatement(connection.prepareStatement(sql));
                if (consumer != null) {
                    consumer.accept(statement);
                }
                resultSet = statement.executeQuery();
                return SQLite3Utils.parseQueryResult(resultSet);
            } catch (Throwable exception) {
                throw new SQLite3Exception(String.format("execute query failed, sql: %s", sql), exception);
            } finally {
                SQLite3Utils.close(resultSet);
                SQLite3Utils.close(statement);
            }
        });
    }

    /**
     * 判断数据表是否存在
     * @param tableName 表名称
     * @return 数据表是否存在（true-存在，false-不存在）
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean isTableExists(String tableName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='table' AND M.name=?", statement -> {
            statement.setString(1, tableName);
        }) == 1;
    }

    /**
     * 判断数据表字段是否存在
     * @param tableName 表名称
     * @param columnName 字段名称
     * @return 数据表字段是否存在（true-存在，false-不存在）
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean isTableColumnExists(String tableName, String columnName) throws SQLite3Exception {
        if (isTableExists(tableName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='table' AND M.name=?", statement -> {
                statement.setString(1, tableName);
            }).contains(columnName);
        }
        return false;
    }

    /**
     * 判断视图是否存在
     * @param viewName 视图名称
     * @return 视图是否存在（true-存在，false-不存在）
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean isViewExists(String viewName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='view' AND M.name=?", statement -> {
            statement.setString(1, viewName);
        }) == 1;
    }

    /**
     * 判断索引是否存在
     * @param indexName 索引名称
     * @return 索引是否存在（true-存在，false-不存在）
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean isIndexExists(String indexName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='index' AND M.name=?", statement -> {
            statement.setString(1, indexName);
        }) == 1;
    }

    /**
     * 判断触发器是否存在
     * @param triggerName 触发器名称
     * @return 触发器是否存在（true-存在，false-不存在）
     * @throws SQLite3Exception SQLite3Exception
     */
    public boolean isTriggerExists(String triggerName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='trigger' AND M.name=?", statement -> {
            statement.setString(1, triggerName);
        }) == 1;
    }

    /**
     * 删除表
     * @param tableName 表名称
     * @return 操作返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int dropTable(String tableName) throws SQLite3Exception {
        return executeUpdate("DROP TABLE " + tableName);
    }

    /**
     * 删除表字段
     * @param tableName 表名称
     * @param columnName 字段名称
     * @return 操作返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int dropTableColumn(String tableName, String columnName) throws SQLite3Exception {
        return executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN " + columnName);
    }

    /**
     * 删除索引
     * @param indexName 索引名称
     * @return  操作返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int dropIndex(String indexName) throws SQLite3Exception {
        return executeUpdate("DROP INDEX " + indexName);
    }

    /**
     * 删除视图
     * @param viewName 视图名称
     * @return 操作返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int dropView(String viewName) throws SQLite3Exception {
        return executeUpdate("DROP VIEW " + viewName);
    }

    /**
     * 删除触发器
     * @param triggerName 触发器名称
     * @return 操作返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int dropTrigger(String triggerName) throws SQLite3Exception {
        return executeUpdate("DROP TRIGGER " + triggerName);
    }

    /**
     * 获取表定义DDL
     * @param tableName 表名称
     * @return 表定义DDL
     * @throws SQLite3Exception SQLite3Exception
     */
    public String getTableDefineSQL(String tableName) throws SQLite3Exception {
        if (isTableExists(tableName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='table' AND M.name=?", statement -> {
                statement.setString(1, tableName);
            });
        }
        return null;
    }

    /**
     * 获取索引定义DDL
     * @param indexName 索引名称
     * @return 索引定义DDL
     * @throws SQLite3Exception SQLite3Exception
     */
    public String getIndexDefineSQL(String indexName) throws SQLite3Exception {
        if (isIndexExists(indexName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='index' AND M.name=?", statement -> {
                statement.setString(1, indexName);
            });
        }
        return null;
    }

    /**
     * 获取视图定义DDL
     * @param viewName 视图名称
     * @return 视图定义DDL
     * @throws SQLite3Exception SQLite3Exception
     */
    public String getViewDefineSQL(String viewName) throws SQLite3Exception {
        if (isViewExists(viewName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='view' AND M.name=?", statement -> {
                statement.setString(1, viewName);
            });
        }
        return null;
    }

    /**
     * 获取触发器定义DDL
     * @param triggerName 触发器名称
     * @return 触发器定义DDL
     * @throws SQLite3Exception SQLite3Exception
     */
    public String getTriggerDefineSQL(String triggerName) throws SQLite3Exception {
        if (isTriggerExists(triggerName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='trigger' AND M.name=?", statement -> {
                statement.setString(1, triggerName);
            });
        }
        return null;
    }

    /**
     * 查询数据表总条数
     * @param tableName 表名称
     * @return 总条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int queryTableRowCount(String tableName) throws SQLite3Exception {
        if (!isTableExists(tableName)) {
            return 0;
        }
        return queryForInt("SELECT COUNT(1) FROM " + tableName);
    }

    /**
     * 查询数据表列的元数据信息
     * @param tableName 表名称
     * @return 数据表列的元数据信息
     * @throws SQLite3Exception SQLite3Exception
     */
    public Map<String, ColumnMetadata> queryTableColumnMetadata(String tableName) throws SQLite3Exception {
        String sql = String.format("SELECT * FROM %s LIMIT 0", tableName);
        return read(connection -> {
            SQLite3PreparedStatement statement = null;
            ResultSet resultSet = null;
            try {
                statement = new SQLite3PreparedStatement(connection.prepareStatement(sql));
                resultSet = statement.executeQuery();
                return SQLite3Utils.parseColumnMetadata(resultSet);
            } catch (Throwable exception) {
                throw new SQLite3Exception(String.format("query table column message failed, sql: %s", sql), exception);
            } finally {
                SQLite3Utils.close(resultSet);
                SQLite3Utils.close(statement);
            }
        });
    }

    /**
     * 批量事务处理（不可重入）（嵌套事务，例如执行多个insert，需使用当前doTransaction进行包裹处理）
     * @param supplier 返回值supplier
     * @param <R> 返回值类型
     * @return 返回值
     * @throws SQLite3Exception SQLite3Exception
     */
    public <R> R doTransaction(Supplier<R> supplier) throws SQLite3Exception {
        logger.debug("==>>批量事务处理");
        boolean hasPrevTransaction = isInTxMode();
        SQLite3Connection sqLite3Connection = fetchWriteConnection();
        setTxMode(sqLite3Connection);
        return write(sqLite3Connection, connection -> {
            try {
                if (!hasPrevTransaction) {
                    connection.setAutoCommit(false);
                }
                try {
                    return supplier.get();
                } catch (Throwable exception) {
                    throw new SQLite3Exception("doTransaction execute failed", exception);
                } finally {
                    if (!hasPrevTransaction) {
                        connection.commit();
                    }
                }
            } catch (Throwable exception) {
                if (!hasPrevTransaction) {
                    try {
                        connection.rollback();
                    } catch (SQLException exception1) {
                        throw new SQLite3Exception(String.format(
                                "doTransaction failed (rollback failed, reason: %s.)"
                                , exception1.getMessage()), exception);
                    }
                    throw new SQLite3Exception("doTransaction failed(rollback success)", exception);
                }
                throw new SQLite3Exception("doTransaction failed(transaction-mode => ignore rollback)", exception);
            } finally {
                if (!hasPrevTransaction) {
                    resetTxMode();
                }
            }
        });
    }

    /**
     * 批量事务处理（不可重入）（嵌套事务，例如执行多个insert，需使用当前doTransaction进行包裹处理）
     * @param voidFunc 无参无返回值Function
     * @throws SQLite3Exception SQLite3Exception
     */
    public void doTransaction(VoidFunc voidFunc) throws SQLite3Exception {
        doTransaction(() -> {
            voidFunc.apply();
            return EMPTY_OBJECT;
        });
    }

//    public int update(Object object) throws SQLite3Exception {
//        List<Object> objectList = new ArrayList<>(1);
//        objectList.add(object);
//        return update(objectList);
//    }
//
//    public int update(List<?> objList) throws SQLite3Exception {
//        return updateOrInsert(objList, TableModel::getUpdateSQL);
//    }
//
//    private int updateOrInsert(List<?> objList, Function<TableModel, String> sqlFunc) throws SQLite3Exception {
//        int retValue = 0;
//        List<?> $objList = Objects.requireNonNull(objList);
//        if (!$objList.isEmpty()) {
//            List<String> sqlList = new ArrayList<>();
//            List<Consumer<SQLite3PreparedStatement>> consumerList = new ArrayList<>();
//            $objList.forEach(object -> {
//                Object $object = Objects.requireNonNull(object);
//                Class<?> objClass = $object.getClass();
//                TableModel tableModel = SQLite3Utils.getClassTableModel(objClass);
//                Map<String, ColumnMetadata> columnMetadata = tableModel.getColumnMetadata();
//                if (columnMetadata == null) {
//                    columnMetadata = queryTableColumnMetadata(tableModel.getTableName());
//                    tableModel.setColumnMetadata(columnMetadata);
//                }
//                sqlList.add(sqlFunc.apply(tableModel));
//                consumerList.add(SQLite3Utils.buildTableConsumer($object, tableModel));
//            });
//            int[] intArr = batchUpdate(sqlList.toArray(new String[0]), (index, statement) -> {
//                consumerList.get(index).accept(statement);
//            });
//            for (int value: intArr) {
//                retValue += value;
//            }
//        }
//        return retValue;
//    }
//
//    public int insert(Object object) throws SQLite3Exception {
//        List<Object> objectList = new ArrayList<>(1);
//        objectList.add(object);
//        return insert(objectList);
//    }
//
//    public int insert(List<?> objList) throws SQLite3Exception {
//        return updateOrInsert(objList, TableModel::getInsertSQL);
//    }

    /**
     * 批量更新处理（多个不同sql）
     * @param sqlArr 待执行sql语句数组
     * @return 批量更新返回int值数组
     * @throws SQLite3Exception SQLite3Exception
     */
    public int[] batchUpdate(String[] sqlArr) throws SQLite3Exception {
        return batchUpdate(sqlArr, (index, statement) -> {});
    }

    /**
     * 批量更新处理（多个不同sql）
     * @param sqlArr 待执行sql语句（占位）数组
     * @param consumer sql语句预编译处理（输入-序号（从0开始）+预编译处理对账）
     * @return 批量更新返回int值数组
     * @throws SQLite3Exception SQLite3Exception
     */
    public int[] batchUpdate(String[] sqlArr, BiConsumer<Integer, SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        logger.debug("==>>批量更新处理（多个不同sql）");
        return doTransaction(() -> write(connection -> {
            int[] effectedRowArr = new int[sqlArr.length];
            SQLite3PreparedStatement statement = null;
            try {
                for (int index = 0; index < sqlArr.length; index++) {
                    String sql = sqlArr[index];
                    try {
                        logger.debug("==>>批量更新处理（多个不同sql），执行sql：{}", sql);
                        statement = new SQLite3PreparedStatement(connection.prepareStatement(sql));
                        if (consumer != null) {
                            consumer.accept(index, statement);
                        }
                        effectedRowArr[index] = statement.executeUpdate();
                    } catch (Throwable exception) {
                        throw new SQLite3Exception(String.format("execute batch update failed, single sql: %s", sql), exception);
                    } finally {
                        SQLite3Utils.close(statement);
                        statement = null;
                    }
                }
                return effectedRowArr;
            } finally {
                SQLite3Utils.close(statement);
            }
        }));
    }

    /**
     * 批量更新处理（一组相同sql）
     * @param sql 待执行sql语句（占位）
     * @param rowCount 批量更新条数
     * @param consumer sql语句预编译处理（输入-序号（从0开始）+预编译处理对账）
     * @return 批量更新返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int batchUpdate(String sql, int rowCount, BiConsumer<Integer, SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        logger.debug("==>>批量更新处理（一组相同sql）");
        return doTransaction(() -> write(connection -> {
            int effectedRowCount = 0;
            SQLite3PreparedStatement statement = null;
            try {
                logger.debug("==>>批量更新处理（一组相同sql），执行sql：{}", sql);
                statement = new SQLite3PreparedStatement(connection.prepareStatement(sql));
                for (int i = 0; i < rowCount; i++) {
                    if (consumer != null) {
                        consumer.accept(i, statement);
                    }
                    effectedRowCount += statement.executeUpdate();
                    statement.clearParameters();
                }
                return effectedRowCount;
            } catch (Throwable exception) {
                throw new SQLite3Exception(String.format("execute batch update failed, sql: %s", sql), exception);
            } finally {
                SQLite3Utils.close(statement);
            }
        }));
    }

    /**
     * 单笔更新处理
     * @param sql 待执行sql语句
     * @return 更新返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int executeUpdate(String sql) throws SQLite3Exception {
        return executeUpdate(sql, s -> {});
    }

    /**
     * 单笔更新处理
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @return 更新返回int值
     * @throws SQLite3Exception SQLite3Exception
     */
    public int executeUpdate(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        logger.debug("==>>单笔更新处理");
        return doTransaction(() -> write(connection -> {
            SQLite3PreparedStatement statement = null;
            try {
                logger.debug("==>>单笔更新处理，执行sql：{}", sql);
                statement = new SQLite3PreparedStatement(connection.prepareStatement(sql));
                if (consumer != null) {
                    consumer.accept(statement);
                }
                return statement.executeUpdate();
            } catch (Throwable exception) {
                throw new SQLite3Exception(String.format("execute update failed, sql: %s", sql), exception);
            } finally {
                SQLite3Utils.close(statement);
            }
        }));
    }

}
