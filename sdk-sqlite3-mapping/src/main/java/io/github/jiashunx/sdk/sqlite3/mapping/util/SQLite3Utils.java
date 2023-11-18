package io.github.jiashunx.sdk.sqlite3.mapping.util;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3PreparedStatement;
import io.github.jiashunx.sdk.sqlite3.metadata.ColumnMetadata;
import io.github.jiashunx.sdk.sqlite3.metadata.QueryResult;
import io.github.jiashunx.sdk.sqlite3.metadata.QueryRetClassModel;
import io.github.jiashunx.sdk.sqlite3.metadata.QueryRetColumnModel;
import io.github.jiashunx.sdk.sqlite3.metadata.TableColumnModel;
import io.github.jiashunx.sdk.sqlite3.metadata.TableModel;
import io.github.jiashunx.sdk.sqlite3.metadata.annotation.SQLite3Column;
import io.github.jiashunx.sdk.sqlite3.metadata.annotation.SQLite3Id;
import io.github.jiashunx.sdk.sqlite3.metadata.annotation.SQLite3Table;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * SQLite3工具类
 * @author jiashunx
 */
public class SQLite3Utils extends io.github.jiashunx.sdk.sqlite3.core.util.SQLite3Utils {

    /**
     * className与对应查询结果映射类模型信息缓存
     */
    private static final Map<String, QueryRetClassModel> QUERY_RET_CLASS_MAP = new HashMap<>();

    /**
     * className与对应数据表模型映射关系缓存
     */
    private static final Map<String, TableModel> CLASS_TABLE_MAP = new HashMap<>();

    /**
     * 私有构造方法
     */
    private SQLite3Utils() {}

    /**
     * 解析并映射查询结果
     * @param queryResult 查询结果封装
     * @param klass 待映射class对象
     * @param <R> 返回结果类型
     * @return 查询结果List
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public static <R> List<R> parseQueryResult(QueryResult queryResult, Class<R> klass) throws NullPointerException, SQLite3Exception {
        if (queryResult == null || klass == null) {
            throw new NullPointerException();
        }
        String klassName = klass.getName();
        QueryRetClassModel retClassModel = getClassQueryRetModel(klass);
        Map<String, QueryRetColumnModel> retColumnModelMap = retClassModel.getRetColumnModelMap();
        List<Map<String, Object>> retMapList = queryResult.getRetMapList();
        Map<String, ColumnMetadata> columnMetadataMap = queryResult.getColumnMetadataMap();
        List<R> retObjList = null;
        if (retMapList != null) {
            AtomicReference<List<R>> retObjListRef = new AtomicReference<>(new ArrayList<>(retMapList.size()));
            retMapList.forEach(rowMap -> {
                R instance = null;
                try {
                    instance = klass.newInstance();
                } catch (Throwable throwable) {
                    throw new SQLite3Exception(String.format("create class[%s] instance failed.", klassName), throwable);
                }
                AtomicReference<R> instanceRef = new AtomicReference<>(instance);
                rowMap.forEach((columnName, columnValue) -> {
                    QueryRetColumnModel retColumnModel = retColumnModelMap.get(columnName);
                    if (retColumnModel != null) {
                        ColumnMetadata columnMetadata = columnMetadataMap.get(columnName);
                        Class<?> fieldType = retColumnModel.getFieldType();
                        if (fieldType == String.class) {
                            retColumnModel.setFieldValue(instanceRef.get(), (String) columnValue);
                        } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                            retColumnModel.setFieldValue(instanceRef.get(), (Boolean) columnValue);
                        }
                        if (fieldType == java.util.Date.class) {
                            int columnTypeOfMetadata = columnMetadata.getColumnType();
                            switch (columnTypeOfMetadata) {
                                case Types.DATE:
                                    retColumnModel.setFieldValue(instanceRef.get(), transferDate((java.sql.Date) columnValue));
                                    break;
                                case Types.TIME:
                                    retColumnModel.setFieldValue(instanceRef.get(), transferTime((java.sql.Time) columnValue));
                                    break;
                                case Types.TIMESTAMP:
                                    retColumnModel.setFieldValue(instanceRef.get(), transferTimestamp((java.sql.Timestamp) columnValue));
                                    break;
                                default:
                                    retColumnModel.setFieldValue(instanceRef.get(), columnValue);
                                    break;
                            }
                        } else if (fieldType == BigDecimal.class) {
                            retColumnModel.setFieldValue(instanceRef.get(), BigDecimal.valueOf(Double.parseDouble(String.valueOf(columnValue))));
                        } else {
                            retColumnModel.setFieldValue(instanceRef.get(), columnValue);
                        }
                    }
                });
                retObjListRef.get().add(instanceRef.get());
            });
            retObjList = retObjListRef.get();
        }
        return retObjList;
    }

    /**
     * 根据class获取对应查询结果映射类模型信息
     * @param klass 待映射class对象
     * @return 查询结果映射类模型信息
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public static QueryRetClassModel getClassQueryRetModel(Class<?> klass) throws NullPointerException, SQLite3Exception {
        if (klass == null) {
            throw new NullPointerException();
        }
        String klassName = klass.getName();
        QueryRetClassModel retClassModel = QUERY_RET_CLASS_MAP.get(klassName);
        if (retClassModel == null) {
            synchronized (SQLite3Utils.class) {
                retClassModel = QUERY_RET_CLASS_MAP.get(klassName);
                if (retClassModel == null) {
                    try {
                        Field[] fields = klass.getDeclaredFields();
                        if (fields.length == 0) {
                            throw new SQLite3Exception(String.format("class[%s] has no declared fields", klassName));
                        }
                        Map<String, QueryRetColumnModel> retColumnModelMap = new HashMap<>();
                        for (Field field: fields) {
                            String fieldName = field.getName();
                            SQLite3Column columnAnnotation = field.getAnnotation(SQLite3Column.class);
                            if (columnAnnotation != null) {
                                String columnName = columnAnnotation.columnName().trim();
                                if (columnName.isEmpty()) {
                                    throw new SQLite3Exception(String.format(
                                            "class[%s] field [%s] has @SQLite3Column annotation, but columnName is empty"
                                            , klassName, fieldName));
                                }
                                QueryRetColumnModel retColumnModel = new QueryRetColumnModel();
                                retColumnModel.setKlassName(klassName);
                                retColumnModel.setColumnName(columnName);
                                retColumnModel.setField(field);
                                retColumnModel.setFieldName(fieldName);
                                retColumnModel.setFieldType(field.getType());
                                retColumnModelMap.put(columnName, retColumnModel);
                            }
                        }
                        if (retColumnModelMap.isEmpty()) {
                            throw new SQLite3Exception(String.format("class[%s] has no field with annotation: @SQLite3Column", klassName));
                        }
                        retClassModel = new QueryRetClassModel();
                        retClassModel.setKlass(klass);
                        retClassModel.setRetColumnModelMap(retColumnModelMap);
                        QUERY_RET_CLASS_MAP.put(klassName, retClassModel);
                    } catch (SecurityException exception) {
                        throw new SQLite3Exception(String.format("visit class[%s] fields failed.", klassName), exception);
                    } catch (Throwable throwable) {
                        if (throwable instanceof SQLite3Exception) {
                            throw (SQLite3Exception) throwable;
                        }
                        throw new SQLite3Exception(throwable);
                    }
                }
            }
        }
        return retClassModel;
    }

    /**
     * 根据class获取对应数据表模型信息
     * @param klass 待映射class对象
     * @return 数据表模型信息
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public static TableModel getClassTableModel(Class<?> klass) throws NullPointerException, SQLite3Exception {
        if (klass == null) {
            throw new NullPointerException();
        }
        String klassName = klass.getName();
        TableModel tableModel = CLASS_TABLE_MAP.get(klassName);
        if (tableModel == null) {
            synchronized (SQLite3Utils.class) {
                tableModel = CLASS_TABLE_MAP.get(klassName);
                if (tableModel == null) {
                    try {
                        SQLite3Table tableAnnotation = klass.getAnnotation(SQLite3Table.class);
                        if (tableAnnotation == null) {
                            throw new SQLite3Exception(String.format("class[%s] doesn't have @SQLite3Table annotation", klassName));
                        }
                        String tableName = tableAnnotation.tableName().trim();
                        if (tableName.isEmpty()) {
                            throw new SQLite3Exception(String.format("class[%s] has @SQLite3Table annotation, but tableName is empty", klassName));
                        }
                        Field[] fields = klass.getDeclaredFields();
                        if (fields.length == 0) {
                            throw new SQLite3Exception(String.format("class[%s] has no declared fields", klassName));
                        }
                        TableColumnModel idColumnModel = null;
                        Map<String, TableColumnModel> columnModelMap = new HashMap<>();
                        for (Field field: fields) {
                            String fieldName = field.getName();
                            SQLite3Column columnAnnotation = field.getAnnotation(SQLite3Column.class);
                            SQLite3Id idAnnotation = field.getAnnotation(SQLite3Id.class);
                            if (idAnnotation != null && columnAnnotation == null) {
                                throw new SQLite3Exception(String.format(
                                        "class[%s] field [%s] has @SQLite3Id annotation, but has no @SQLite3Column annotation"
                                        , klassName, fieldName));
                            }
                            if (columnAnnotation != null) {
                                String columnName = columnAnnotation.columnName().trim();
                                if (columnName.isEmpty()) {
                                    throw new SQLite3Exception(String.format(
                                            "class[%s] field [%s] has @SQLite3Column annotation, but columnName is empty"
                                            , klassName, fieldName));
                                }
                                TableColumnModel columnModel = new TableColumnModel();
                                columnModel.setKlassName(klassName);
                                columnModel.setTableName(tableName);
                                columnModel.setColumnName(columnName);
                                columnModel.setField(field);
                                columnModel.setFieldName(fieldName);
                                columnModel.setFieldType(field.getType());
                                columnModel.setIdColumn(false);
                                if (columnModelMap.containsKey(columnName)) {
                                    throw new SQLite3Exception(String.format(
                                            "class[%s] has more than one field mapping to table column: %s"
                                            , klassName, columnName));
                                }
                                if (idAnnotation != null) {
                                    if (idColumnModel != null) {
                                        throw new SQLite3Exception(String.format(
                                                "class[%s] has more than one field with @SQLite3Id annotation, such as %s, %s"
                                                , klassName, idColumnModel.getFieldName(), fieldName));
                                    }
                                    columnModel.setIdColumn(true);
                                    idColumnModel = columnModel;
                                }
                                columnModelMap.put(columnName, columnModel);
                            }
                        }
                        if (idColumnModel == null) {
                            throw new SQLite3Exception(String.format("class[%s] has no field with annotation: @SQLite3Id", klassName));
                        }
                        List<TableColumnModel> columnModelList = new ArrayList<>(columnModelMap.size());
                        columnModelMap.values().forEach(columnModel -> {
                            if (columnModel.isIdColumn()) {
                                return;
                            }
                            columnModelList.add(columnModel);
                        });
                        columnModelList.add(idColumnModel);
                        tableModel = new TableModel();
                        tableModel.setKlass(klass);
                        tableModel.setKlassName(klassName);
                        tableModel.setTableName(tableName);
                        tableModel.setIdColumnModel(idColumnModel);
                        tableModel.setColumnModelMap(columnModelMap);
                        tableModel.setColumnModelList(columnModelList);
                        CLASS_TABLE_MAP.put(klassName, tableModel);
                    } catch (SecurityException exception) {
                        throw new SQLite3Exception(String.format("visit class[%s] fields failed.", klassName), exception);
                    } catch (Throwable throwable) {
                        if (throwable instanceof SQLite3Exception) {
                            throw (SQLite3Exception) throwable;
                        }
                        throw new SQLite3Exception(throwable);
                    }
                }
            }
        }
        return tableModel;

    }

    /**
     * 根据模型实例及数据表模型信息封装数据对象预处理赋值处理（处理模型所有字段）
     * @param object 模型实例
     * @param tableModel 数据表模型信息
     * @return 预处理赋值处理
     */
    public static Consumer<SQLite3PreparedStatement> buildTableConsumer(Object object, TableModel tableModel) {
        return buildTableConsumer(object, tableModel, (tableColumnModel, columnMetadata) -> true);
    }

    /**
     * 根据模型实例及数据表模型信息封装数据对象预处理赋值处理（可指定过滤字段）
     * @param object 模型实例
     * @param tableModel 数据表模型信息
     * @param filter 字段过滤处理
     * @return 预处理赋值处理
     */
    public static Consumer<SQLite3PreparedStatement> buildTableConsumer(Object object, TableModel tableModel, BiPredicate<TableColumnModel, ColumnMetadata> filter) {
        if (object == null || tableModel == null) {
            throw new NullPointerException();
        }
        return statement -> {
            String tableName = tableModel.getTableName();
            Map<String, ColumnMetadata> columnMetadataMap = tableModel.getColumnMetadata();
            List<TableColumnModel> columnModelList = tableModel.getColumnModelList();
            for (int index = 0, size = columnModelList.size(); index < size; index++) {
                TableColumnModel columnModel = columnModelList.get(index);
                String columnName = columnModel.getColumnName();
                ColumnMetadata columnMetadata = columnMetadataMap.get(columnName);
                if (columnMetadata == null) {
                    throw new SQLite3Exception(String.format("table[%s] has no field: %s", tableName, columnName));
                }
                if (!filter.test(columnModel, columnMetadata)) {
                    continue;
                }
                int insertIndex = index + 1;
                Object value = columnModel.getFieldValue(object);
                String stringValue = String.valueOf(value);
                Class<?> fieldType = columnModel.getFieldType();
                if (fieldType == String.class || fieldType == char.class || fieldType == Character.class) {
                    statement.setString(insertIndex, (String) value);
                } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                    statement.setBoolean(insertIndex, Boolean.parseBoolean(stringValue));
                } else if (fieldType == byte.class || fieldType == Byte.class) {
                    statement.setByte(insertIndex, Byte.parseByte(stringValue));
                } else if (fieldType == short.class || fieldType == Short.class) {
                    statement.setShort(insertIndex, Short.parseShort(stringValue));
                } else if (fieldType == int.class || fieldType == Integer.class) {
                    statement.setInt(insertIndex, Integer.parseInt(stringValue));
                } else if (fieldType == float.class || fieldType == Float.class) {
                    statement.setFloat(insertIndex, Float.parseFloat(stringValue));
                } else if (fieldType == double.class || fieldType == Double.class) {
                    statement.setDouble(insertIndex, Double.parseDouble(stringValue));
                } else if (fieldType == long.class || fieldType == Long.class) {
                    statement.setLong(insertIndex, Long.parseLong(stringValue));
                } else if (fieldType == BigDecimal.class) {
                    statement.setBigDecimal(insertIndex, (BigDecimal) value);
                } else if (fieldType == byte[].class) {
                    statement.setBytes(insertIndex, (byte[]) value);
                } else if (InputStream.class.isAssignableFrom(fieldType)) {
                    statement.setBlob(insertIndex, (InputStream) value);
                } else if (fieldType == Blob.class) {
                    statement.setBlob(insertIndex, (Blob) value);
                } else if (Reader.class.isAssignableFrom(fieldType)) {
                    statement.setClob(insertIndex, (Reader) value);
                } else if (fieldType == Clob.class) {
                    statement.setClob(insertIndex, (Clob) value);
                } else if (fieldType == NClob.class) {
                    statement.setNClob(insertIndex, (NClob) value);
                } else if (fieldType == java.util.Date.class) {
                    switch (columnMetadata.getColumnTypeName()) {
                        case "DATE":
                            statement.setDate(insertIndex, transferDate((java.util.Date) value));
                            break;
                        case "TIME":
                            statement.setTime(insertIndex, transferTime((java.util.Date) value));
                            break;
                        case "TIMESTAMP":
                            statement.setTimestamp(insertIndex, transferTimestamp((java.util.Date) value));
                            break;
                        default:
                            statement.setObject(insertIndex, value);
                            break;
                    }
                } else if (fieldType == java.sql.Date.class) {
                    statement.setDate(insertIndex, (java.sql.Date) value);
                } else if (fieldType == java.sql.Time.class) {
                    statement.setTime(insertIndex, (java.sql.Time) value);
                } else if (fieldType == java.sql.Timestamp.class) {
                    statement.setTimestamp(insertIndex, (java.sql.Timestamp) value);
                } else {
                    statement.setObject(insertIndex, value);
                }
            }
        };
    }

}
