package io.github.jiashunx.sdk.sqlite3.metadata;

import java.lang.reflect.Field;

/**
 * 查询返回结果映射列模型信息
 * @author jiashunx
 */
public class QueryRetColumnModel {

    /**
     * class名称.
     */
    private String klassName;

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

    /**
     * 设置模型字段值
     * @param object 模型实例
     * @param value 模型字段值
     */
    public void setFieldValue(Object object, Object value) {
        try {
            field.setAccessible(true);
            field.set(object, value);
        } catch (Throwable throwable) {
            throw new RuntimeException(String.format(
                    "set class[%s] field[%s] value faile.", klassName, fieldName), throwable);
        }
    }

    public String getKlassName() {
        return klassName;
    }

    public void setKlassName(String klassName) {
        this.klassName = klassName;
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
