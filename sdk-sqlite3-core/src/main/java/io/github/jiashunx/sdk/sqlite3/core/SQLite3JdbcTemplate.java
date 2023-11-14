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

    private static final Object EMPTY_OBJECT = new byte[0];

    private static final ThreadLocal<Boolean> TX_MODE = new ThreadLocal<>();

    private static final ThreadLocal<SQLite3Connection> TX_CONNECTION = new ThreadLocal<>();

    public static boolean isInTransactionModel() {
        return TX_MODE.get() != null && TX_MODE.get();
    }

    public static SQLite3Connection getTxConnection() {
        return TX_CONNECTION.get();
    }

    private static void setTxMode(SQLite3Connection connection) {
        if (isInTransactionModel() && getTxConnection() != connection) {
            throw new SQLite3Exception("transaction connection conflict.");
        }
        TX_CONNECTION.set(Objects.requireNonNull(connection));
        TX_MODE.set(true);
    }

    private static void resetTxMode() {
        TX_CONNECTION.remove();
        TX_MODE.remove();
    }

    private SQLite3ConnectionPool connectionPool;

    public SQLite3JdbcTemplate(String fileName) {
        this(SQLite3ConnectionPoolManager.create(fileName));
    }

    public SQLite3JdbcTemplate(SQLite3ConnectionPool pool) {
        this();
        this.connectionPool = pool;
    }

    public SQLite3JdbcTemplate() {}

    private SQLite3ConnectionPool getConnectionPool() {
        return Objects.requireNonNull(connectionPool);
    }

    private SQLite3Connection getWriteConnection() {
        if (isInTransactionModel()) {
            return getTxConnection();
        }
        return getConnectionPool().fetchWriteConnection();
    }

    private void write(Consumer<Connection> consumer) {
        write(getWriteConnection(), consumer);
    }

    private void write(SQLite3Connection connection, Consumer<Connection> consumer) {
        connection.write(c -> {
            try {
                consumer.accept(c);
            } finally {
                if (!isInTransactionModel()) {
                    connection.release();
                }
            }
        });
    }

    private <R> R write(Function<Connection, R> function) {
        return write(getWriteConnection(), function);
    }

    private <R> R write(SQLite3Connection connection, Function<Connection, R> function) {
        return connection.write(c -> {
            try {
                return function.apply(c);
            } finally {
                if (!isInTransactionModel()) {
                    connection.release();
                }
            }
        });
    }

    private SQLite3Connection getReadConnection() {
        if (isInTransactionModel()) {
            return getTxConnection();
        }
        return getConnectionPool().fetchReadConnection();
    }

    private void query(Consumer<Connection> consumer) {
        query(getReadConnection(), consumer);
    }

    private void query(SQLite3Connection connection, Consumer<Connection> consumer) {
        connection.read(c -> {
            try {
                consumer.accept(c);
            } finally {
                if (!isInTransactionModel()) {
                    connection.release();
                }
            }
        });
    }

    private <R> R query(Function<Connection, R> function) {
        return query(getReadConnection(), function);
    }

    private <R> R query(SQLite3Connection connection, Function<Connection, R> function) {
        return connection.read(c -> {
            try {
                return function.apply(c);
            } finally {
                if (!isInTransactionModel()) {
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

    public boolean queryForBoolean(String sql) throws SQLite3Exception {
        return queryForBoolean(sql, s -> {});
    }

    public boolean queryForBoolean(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Boolean.parseBoolean(queryForString(sql, consumer));
    }

    public byte queryForByte(String sql) throws SQLite3Exception {
        return queryForByte(sql, s -> {});
    }

    public byte queryForByte(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Byte.parseByte(queryForString(sql, consumer));
    }

    public short queryForShort(String sql) throws SQLite3Exception {
        return queryForShort(sql, s -> {});
    }

    public short queryForShort(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Short.parseShort(queryForString(sql, consumer));
    }

    public int queryForInt(String sql) throws SQLite3Exception {
        return queryForInt(sql, s -> {});
    }

    public int queryForInt(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Integer.parseInt(queryForString(sql, consumer));
    }

    public float queryForFloat(String sql) throws SQLite3Exception {
        return queryForFloat(sql, s -> {});
    }

    public float queryForFloat(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Float.parseFloat(queryForString(sql, consumer));
    }

    public double queryForDouble(String sql) throws SQLite3Exception {
        return queryForDouble(sql, s -> {});
    }

    public double queryForDouble(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return Double.parseDouble(queryForString(sql, consumer));
    }

    public String queryForString(String sql) throws SQLite3Exception {
        return queryForString(sql, s -> {});
    }

    public String queryForString(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return queryForOneValue(sql, consumer).toString();
    }

    public Object queryForOneValue(String sql) throws SQLite3Exception {
        return queryForOneValue(sql, s -> {});
    }

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

    public Map<String, Object> queryForMap(String sql) throws SQLite3Exception {
        return queryForMap(sql, s -> {});
    }

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

    public List<Map<String, Object>> queryForList(String sql) throws SQLite3Exception {
        return queryForList(sql, s -> {});
    }

    public List<Map<String, Object>> queryForList(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return queryForResult(sql, consumer).getRetMapList();
    }

    public QueryResult queryForResult(String sql) throws SQLite3Exception {
        return queryForResult(sql, s -> {});
    }

    public QueryResult queryForResult(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return query(connection -> {
            SQLite3PreparedStatement statement = null;
            ResultSet resultSet = null;
            try {
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

    public boolean isTableExists(String tableName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='table' AND M.name=?", statement -> {
            statement.setString(1, tableName);
        }) == 1;
    }

    public boolean isTableColumnExists(String tableName, String columnName) throws SQLite3Exception {
        if (isTableExists(tableName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='table' AND M.name=?", statement -> {
                statement.setString(1, tableName);
            }).contains(columnName);
        }
        return false;
    }

    public boolean isViewExists(String viewName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='view' AND M.name=?", statement -> {
            statement.setString(1, viewName);
        }) == 1;
    }

    public boolean isIndexExists(String indexName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='index' AND M.name=?", statement -> {
            statement.setString(1, indexName);
        }) == 1;
    }

    public boolean isTriggerExists(String triggerName) throws SQLite3Exception {
        return queryForInt("SELECT COUNT(1) FROM sqlite_master M WHERE M.type='trigger' AND M.name=?", statement -> {
            statement.setString(1, triggerName);
        }) == 1;
    }

    public int dropTable(String tableName) throws SQLite3Exception {
        return executeUpdate("DROP TABLE " + tableName);
    }

    public int dropTableColumn(String tableName, String columnName) throws SQLite3Exception {
        return executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN " + columnName);
    }

    public int dropIndex(String indexName) throws SQLite3Exception {
        return executeUpdate("DROP INDEX " + indexName);
    }

    public int dropTrigger(String triggerName) throws SQLite3Exception {
        return executeUpdate("DROP TRIGGER " + triggerName);
    }

    public String getTableDefineSQL(String tableName) throws SQLite3Exception {
        if (isTableExists(tableName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='table' AND M.name=?", statement -> {
                statement.setString(1, tableName);
            });
        }
        return null;
    }

    public String getViewDefineSQL(String viewName) throws SQLite3Exception {
        if (isViewExists(viewName)) {
            return queryForString("SELECT M.sql FROM sqlite_master M WHERE M.type='view' AND M.name=?", statement -> {
                statement.setString(1, viewName);
            });
        }
        return null;
    }

    public int queryTableRowCount(String tableName) throws SQLite3Exception {
        if (!isTableExists(tableName)) {
            return 0;
        }
        return queryForInt("SELECT COUNT(1) FROM " + tableName);
    }

    public Map<String, ColumnMetadata> queryTableColumnMetadata(String tableName) throws SQLite3Exception {
        String sql = String.format("SELECT * FROM %s LIMIT 0", tableName);
        return query(connection -> {
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

    public <R> R doTransaction(Supplier<R> supplier) throws SQLite3Exception {
        boolean hasPrevTransaction = isInTransactionModel();
        SQLite3Connection sqLite3Connection = getWriteConnection();
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

    public int[] batchUpdate(String[] sqlArr) throws SQLite3Exception {
        return batchUpdate(sqlArr, (index, statement) -> {});
    }

    public int[] batchUpdate(String[] sqlArr, BiConsumer<Integer, SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return doTransaction(() -> write(connection -> {
            int[] effectedRowArr = new int[sqlArr.length];
            SQLite3PreparedStatement statement = null;
            try {
                for (int index = 0; index < sqlArr.length; index++) {
                    String sql = sqlArr[index];
                    try {
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

    public int batchUpdate(String sql, int rowCount, BiConsumer<Integer, SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return doTransaction(() -> write(connection -> {
            int effectedRowCount = 0;
            SQLite3PreparedStatement statement = null;
            try {
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

    public int executeUpdate(String sql) throws SQLite3Exception {
        return executeUpdate(sql, s -> {});
    }

    public int executeUpdate(String sql, Consumer<SQLite3PreparedStatement> consumer) throws SQLite3Exception {
        return doTransaction(() -> write(connection -> {
            SQLite3PreparedStatement statement = null;
            try {
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
