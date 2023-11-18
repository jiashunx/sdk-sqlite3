package io.github.jiashunx.sdk.sqlite3.metadata;

import java.lang.reflect.Field;

/**
 * 数据表列模型信息
 * @author jiashunx
 */
public class TableColumnModel {

    /**
     * class名称.
     */
    private String klassName;

    /**
     * table名称.
     */
    private String tableName;

    /**
     * 是否主键字段
     */
    private boolean idColumn;

    /**
     * 列名
     */
    private String columnName;

    /**
     * 模型字段名称
     */
    private String fieldName;

    /**
     * 模型字段类型
     */
    private Class<?> fieldType;

    /**
     * 模型字段对象
     */
    private Field field;

    public Object getFieldValue(Object object) {
        try {
            field.setAccessible(true);
            return field.get(object);
        } catch (Throwable throwable) {
            throw new RuntimeException(String.format(
                    "get field %s[class: %s, table: %s] value failed.", fieldName, klassName, tableName), throwable);
        }
    }

    public String getKlassName() {
        return klassName;
    }

    public void setKlassName(String klassName) {
        this.klassName = klassName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isIdColumn() {
        return idColumn;
    }

    public void setIdColumn(boolean idColumn) {
        this.idColumn = idColumn;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Class<?> getFieldType() {
        return fieldType;
    }

    public void setFieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }
}
