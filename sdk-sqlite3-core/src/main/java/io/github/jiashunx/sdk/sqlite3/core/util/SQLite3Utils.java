package io.github.jiashunx.sdk.sqlite3.core.util;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.metadata.ColumnMetadata;
import io.github.jiashunx.sdk.sqlite3.metadata.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQLite3工具类
 * @author jiashunx
 */
public class SQLite3Utils {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3Utils.class);

    private SQLite3Utils() {}

    /**
     * 解析查询返回结果
     * @param resultSet 查询返回ResultSet对象
     * @return 查询返回结果QueryResult对象
     * @throws NullPointerException NullPointerException
     * @throws SQLException SQLException
     * @throws SQLite3Exception SQLite3Exception
     */
    public static QueryResult parseQueryResult(ResultSet resultSet) throws NullPointerException, SQLException, SQLite3Exception {
        if (resultSet == null) {
            throw new NullPointerException();
        }
        List<Map<String, Object>> retMapList = new ArrayList<>();
        Map<String, ColumnMetadata> columnMap = parseColumnMetadata(resultSet);
        while (resultSet.next()) {
            Map<String, Object> rowMap = new HashMap<>();
            for (Map.Entry<String, ColumnMetadata> entry: columnMap.entrySet()) {
                String columnName = entry.getKey();
                ColumnMetadata columnMetadata = entry.getValue();
                String columnLabel = columnMetadata.getColumnLabel();
                Object columnValue = null;
                /**
                 * java.sql.JDBCType
                 */
                switch (columnMetadata.getColumnTypeName()) {
                    case "BOOLEAN":
                    case "BIT":
                        columnValue = resultSet.getBoolean(columnLabel);
                        break;
                    case "INT1":
                    case "TINYINT":
                        columnValue = resultSet.getByte(columnLabel);
                        break;
                    case "INT2":
                    case "SMALLINT":
                        columnValue = resultSet.getShort(columnLabel);
                        break;
                    case "MEDIUMINT":
                    case "INT":
                    case "INT4":
                    case "INTEGER":
                        columnValue = resultSet.getInt(columnLabel);
                        break;
                    case "INT8":
                    case "BIGINT":
                        columnValue = resultSet.getLong(columnLabel);
                        break;
                    case "FLOAT":
                        columnValue = resultSet.getFloat(columnLabel);
                        break;
                    case "REAL":
                    case "DOUBLE":
                        columnValue = resultSet.getDouble(columnLabel);
                        break;
                    case "NUMERIC":
                    case "DECIMAL":
                        columnValue = resultSet.getBigDecimal(columnLabel);
                        break;
                    case "CHAR":
                    case "VARCHAR":
                    case "LONGVARCHAR":
                    case "CLOB":
                    case "TEXT":
                    case "TINYTEXT":
                    case "MEDIUMTEXT":
                    case "LONGTEXT":
                    case "NCHAR":
                    case "NVARCHAR":
                    case "LONGNVARCHAR":
                    case "NCLOB":
                        columnValue = resultSet.getString(columnLabel);
                        break;
                    case "DATE":
                        columnValue = transferDate(resultSet.getDate(columnLabel));
                        break;
                    case "TIME":
                        columnValue = transferTime(resultSet.getTime(columnLabel));
                        break;
                    case "TIMESTAMP":
                        columnValue = transferTimestamp(resultSet.getTimestamp(columnLabel));
                        break;
                    case "BINARY":
                    case "VARBINARY":
                    case "LONGVARBINARY":
                        columnValue = resultSet.getBytes(columnLabel);
                        break;
                    case "BLOB":
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        try (InputStream inputStream = resultSet.getBinaryStream(columnLabel);) {
                            if (inputStream != null) {
                                byte[] buffer = new byte[1024];
                                int temp = 0;
                                while ((temp = inputStream.read(buffer)) >= 0) {
                                    bos.write(buffer, 0, temp);
                                }
                            }
                        } catch (Throwable throwable) {
                            throw new SQLite3Exception(String.format("read blob column[%s] failed.", columnName), throwable);
                        }
                        columnValue = bos.toByteArray();
                        break;
                    default:
                        columnValue = resultSet.getObject(columnLabel);
                        break;
                }
                rowMap.put(columnName, columnValue);
            }
            retMapList.add(rowMap);
        }
        return new QueryResult(columnMap, retMapList);
    }

    /**
     * 从查询结果解析列字段元数据信息
     * @param resultSet 查询返回结果
     * @return 列字段名称与列字段元数据信息映射map
     * @throws NullPointerException NullPointerException
     * @throws SQLException SQLException
     */
    public static Map<String, ColumnMetadata> parseColumnMetadata(ResultSet resultSet) throws NullPointerException, SQLException {
        if (resultSet == null) {
            throw new NullPointerException();
        }
        Map<String, ColumnMetadata> retMap = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int index = 1; index <= columnCount; index++) {
            ColumnMetadata columnMetadata = new ColumnMetadata();
            String columnName = metaData.getColumnName(index);
            columnMetadata.setColumnName(columnName);
            columnMetadata.setColumnLabel(metaData.getColumnLabel(index));
            columnMetadata.setColumnType(metaData.getColumnType(index));
            columnMetadata.setColumnTypeName(metaData.getColumnTypeName(index));
            columnMetadata.setColumnClassName(metaData.getColumnClassName(index));
            columnMetadata.setColumnDisplaySize(metaData.getColumnDisplaySize(index));
            columnMetadata.setCatalogName(metaData.getCatalogName(index));
            columnMetadata.setPrecision(metaData.getPrecision(index));
            columnMetadata.setScale(metaData.getScale(index));
            columnMetadata.setSchemaName(metaData.getSchemaName(index));
            columnMetadata.setTableName(metaData.getTableName(index));
            columnMetadata.setAutoIncrement(metaData.isAutoIncrement(index));
            columnMetadata.setCaseSensitive(metaData.isCaseSensitive(index));
            columnMetadata.setCurrency(metaData.isCurrency(index));
            columnMetadata.setDefinitelyWritable(metaData.isDefinitelyWritable(index));
            columnMetadata.setNullable(metaData.isNullable(index));
            columnMetadata.setReadOnly(metaData.isReadOnly(index));
            columnMetadata.setWritable(metaData.isWritable(index));
            columnMetadata.setSearchable(metaData.isSearchable(index));
            columnMetadata.setSigned(metaData.isSigned(index));
            retMap.put(columnName, columnMetadata);
        }
        return retMap;
    }

    public static void close(Statement statement) {
        close((AutoCloseable) statement);
    }

    public static void close(ResultSet resultSet) {
        close((AutoCloseable) resultSet);
    }

    public static void close(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) {
                logger.error("close AutoCloseable object [{}] failed.", closeable.getClass(), exception);
            }
        }
    }

    public static java.util.Date transferDate(java.sql.Date sqlDate) {
        if (sqlDate == null) {
            return null;
        }
        return new java.util.Date(sqlDate.getTime());
    }

    public static java.util.Date transferTime(java.sql.Time sqlTime) {
        if (sqlTime == null) {
            return null;
        }
        return new java.util.Date(sqlTime.getTime());
    }

    public static java.util.Date transferTimestamp(java.sql.Timestamp sqlTimestamp) {
        if (sqlTimestamp == null) {
            return null;
        }
        return new java.util.Date(sqlTimestamp.getTime());
    }

    public static java.sql.Date transferDate(java.util.Date utilDate) {
        if (utilDate == null) {
            return null;
        }
        return new java.sql.Date(utilDate.getTime());
    }

    public static java.sql.Time transferTime(java.util.Date utilDate) {
        if (utilDate == null) {
            return null;
        }
        return new java.sql.Time(utilDate.getTime());
    }

    public static java.sql.Timestamp transferTimestamp(java.util.Date utilDate) {
        if (utilDate == null) {
            return null;
        }
        return new java.sql.Timestamp(utilDate.getTime());
    }

}
