package io.github.jiashunx.sdk.sqlite3.mapping;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.pool.SQLite3ConnectionPool;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3PreparedStatement;
import io.github.jiashunx.sdk.sqlite3.mapping.util.SQLite3Utils;
import io.github.jiashunx.sdk.sqlite3.metadata.ColumnMetadata;
import io.github.jiashunx.sdk.sqlite3.metadata.TableModel;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.SQLPackage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 封装SQLite3 JDBC操作模型（模型映射）
 * @author jiashunx
 */
public class SQLite3JdbcTemplate extends io.github.jiashunx.sdk.sqlite3.core.SQLite3JdbcTemplate {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3JdbcTemplate.class);

    /**
     * 构造方法
     * @param fileName SQLite3数据库地址
     */
    public SQLite3JdbcTemplate(String fileName) {
        super(fileName);
    }

    /**
     * 构造方法
     * @param pool SQLite3数据库连接池
     */
    public SQLite3JdbcTemplate(SQLite3ConnectionPool pool) {
        super(pool);
    }

    /**
     * 查询并返回模型实例（单个）
     * @param sql 待执行sql语句
     * @param klass 模型Class对象
     * @param <R> 返回对象类型
     * @return 模型实例
     * @throws SQLite3Exception SQLite3Exception
     */
    public <R> R queryForObj(String sql, Class<R> klass) throws SQLite3Exception {
        return queryForObj(sql, statement -> {}, klass);
    }

    /**
     * 查询并返回模型实例（单个）
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @param klass 模型Class对象
     * @param <R> 返回对象类型
     * @return 模型实例
     * @throws SQLite3Exception SQLite3Exception
     */
    public <R> R queryForObj(String sql, Consumer<SQLite3PreparedStatement> consumer, Class<R> klass)
            throws SQLite3Exception {
        List<R> retList = queryForList(sql, consumer, klass);
        if (retList == null || retList.isEmpty()) {
            return null;
        }
        if (retList.size() > 1) {
            throw new SQLite3Exception(String.format("query result contains more than one column, sql: %s", sql));
        }
        return retList.get(0);
    }

    /**
     * 查询并返回模型实例（列表）
     * @param sql 待执行sql语句
     * @param klass 模型Class对象
     * @param <R> 返回对象类型
     * @return 模型实例列表
     * @throws SQLite3Exception SQLite3Exception
     */
    public <R> List<R> queryForList(String sql, Class<R> klass) throws SQLite3Exception {
        return queryForList(sql, statement -> {}, klass);
    }

    /**
     * 查询并返回模型实例（列表）
     * @param sql 待执行sql语句（占位）
     * @param consumer sql语句预编译处理
     * @param klass 模型Class对象
     * @param <R> 返回对象类型
     * @return 模型实例列表
     * @throws SQLite3Exception SQLite3Exception
     */
    public <R> List<R> queryForList(String sql, Consumer<SQLite3PreparedStatement> consumer, Class<R> klass)
            throws SQLite3Exception {
        return SQLite3Utils.parseQueryResult(queryForResult(sql, consumer), klass);
    }

    /**
     * 插入单一模型
     * @param object 模型实例
     * @return 插入条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int insert(Object object) throws SQLite3Exception {
        List<Object> objectList = new ArrayList<>(1);
        objectList.add(object);
        return insert(objectList);
    }

    /**
     * 插入多条模型
     * @param objList 模型实例列表
     * @return 插入条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int insert(List<?> objList) throws SQLite3Exception {
        return execute(objList, TableModel::sqlOfInsert);
    }

    /**
     * 更新单一模型
     * @param object 模型实例
     * @return 更新条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int update(Object object) throws SQLite3Exception {
        List<Object> objectList = new ArrayList<>(1);
        objectList.add(object);
        return update(objectList);
    }

    /**
     * 更新多条模型
     * @param objList 模型实例列表
     * @return 更新条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int update(List<?> objList) throws SQLite3Exception {
        return execute(objList, TableModel::sqlOfUpdate);
    }

    /**
     * 根据主键删除模型
     * @param object 模型实例
     * @return 删除条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int delete(Object object) throws SQLite3Exception {
        List<Object> objectList = new ArrayList<>(1);
        objectList.add(object);
        return delete(objectList);
    }

    /**
     * 根据主键删除模型
     * @param objList 模型实例列表
     * @return 删除条数
     * @throws SQLite3Exception SQLite3Exception
     */
    public int delete(List<?> objList) throws SQLite3Exception {
        return execute(objList, TableModel::sqlOfDeleteById, (object, tableModel) -> SQLite3Utils.buildTableConsumer(object, tableModel, (tcm, cm) -> tcm.isIdColumn()));
    }

    /**
     * 增删改处理（支持多条相同处理，如批量新增|批量更新|批量删除）
     * @param objList 待处理模型实例列表
     * @param sqlFunc sql构造处理
     * @return 增删改条数
     * @throws SQLite3Exception SQLite3Exception
     */
    private int execute(List<?> objList, Function<TableModel, String> sqlFunc) throws SQLite3Exception {
        return execute(objList, sqlFunc, SQLite3Utils::buildTableConsumer);
    }

    /**
     * 增删改处理（支持多条相同处理，如批量新增|批量更新|批量删除）
     * @param objList 待处理模型实例列表
     * @param sqlFunc sql构造处理
     * @param statementFunc sql预处理对象生成器
     * @return 增删改条数
     * @throws SQLite3Exception SQLite3Exception
     */
    private int execute(List<?> objList, Function<TableModel, String> sqlFunc, BiFunction<Object, TableModel
            , Consumer<SQLite3PreparedStatement>> statementFunc) throws SQLite3Exception {
        int retValue = 0;
        List<?> $objList = Objects.requireNonNull(objList);
        if (!$objList.isEmpty()) {
            List<String> sqlList = new ArrayList<>();
            List<Consumer<SQLite3PreparedStatement>> consumerList = new ArrayList<>();
            $objList.forEach(object -> {
                Object $object = Objects.requireNonNull(object);
                Class<?> objClass = $object.getClass();
                TableModel tableModel = SQLite3Utils.getClassTableModel(objClass);
                Map<String, ColumnMetadata> columnMetadata = tableModel.getColumnMetadata();
                if (columnMetadata == null) {
                    columnMetadata = queryTableColumnMetadata(tableModel.getTableName());
                    tableModel.setColumnMetadata(columnMetadata);
                }
                sqlList.add(sqlFunc.apply(tableModel));
                consumerList.add(statementFunc.apply($object, tableModel));
            });
            int[] intArr = batchUpdate(sqlList.toArray(new String[0]), (index, statement) -> {
                consumerList.get(index).accept(statement);
            });
            for (int value: intArr) {
                retValue += value;
            }
        }
        return retValue;
    }

    /**
     * 初始化SQL包（数据结构初始化：表、索引、视图、触发器等）
     * @param sqlPackage SQL包对象
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public void initSQLPackage(SQLPackage sqlPackage) throws NullPointerException, SQLite3Exception {
        if (sqlPackage == null) {
            throw new NullPointerException();
        }
        try {
            sqlPackage.getTableNames().forEach(tableName -> {
                if (!isTableExists(tableName)) {
                    String tableDefineSQL = sqlPackage.getTableDefineSQL(tableName);
                    logger.warn("table[{}] no exists, prepare create it, sql: {}", tableName, tableDefineSQL);
                    executeUpdate(tableDefineSQL);
                }
                sqlPackage.getColumns(tableName).forEach(column -> {
                    String columnName = column.getColumnName();
                    if (!isTableColumnExists(tableName, columnName)) {
                        String columnDefineSQL = sqlPackage.getColumnDefineSQL(tableName, columnName);
                        logger.warn("table[{}] column[{}] not exists, prepare create it, sql: {}", tableName, columnName, columnDefineSQL);
                        executeUpdate(columnDefineSQL);
                    }
                });
            });
            sqlPackage.getViewNames().forEach(viewName -> {
                if (!isViewExists(viewName)) {
                    String viewDefineSQL = sqlPackage.getViewDefineSQL(viewName);
                    logger.warn("view[{}] not exists, prepare create it, sql: {}", viewName, viewDefineSQL);
                    executeUpdate(viewDefineSQL);
                }
            });
            sqlPackage.getIndexNames().forEach(tableName -> {
                sqlPackage.getIndexes(tableName).forEach(index -> {
                    String indexName = index.getIndexName();
                    if (!isIndexExists(indexName)) {
                        String indexDefineSQL = sqlPackage.getIndexDefineSQL(tableName, indexName);
                        logger.warn("table[{}] index[{}] not exists, prepare create it, sql: {}", tableName, indexName, indexDefineSQL);
                        executeUpdate(indexDefineSQL);
                    }
                });
            });
            sqlPackage.getTriggerNames().forEach(triggerName -> {
                if (!isTriggerExists(triggerName)) {
                    String triggerDefineSQL = sqlPackage.getTriggerDefineSQL(triggerName);
                    logger.warn("trigger[{}] not exists, prepare create it, sql: {}", triggerName, triggerDefineSQL);
                    executeUpdate(triggerDefineSQL);
                }
            });
        } catch (Throwable throwable) {
            throw new SQLite3Exception(String.format("init sql package failed, groupId: %s", sqlPackage.getGroupId()), throwable);
        }
    }

}
