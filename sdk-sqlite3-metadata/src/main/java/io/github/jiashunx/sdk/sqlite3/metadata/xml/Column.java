package io.github.jiashunx.sdk.sqlite3.metadata.xml;

import java.util.Objects;

/**
 * XML列模型
 * @author jiashunx
 */
public class Column {

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 字段名称
     */
    private String columnName;

    /**
     * 字段类型
     */
    private String columnType;

    /**
     * 是否主键
     */
    private boolean primary;

    /**
     * 表描述信息
     */
    private String tableDesc;

    /**
     * 列注释
     */
    private String columnComment;

    /**
     * 列字段长度
     */
    private int length;

    /**
     * 是否非空
     */
    private boolean notNull;

    /**
     * 默认值
     */
    private String defaultValue;

    /**
     * 外键关联表名称
     */
    private String foreignTable;

    /**
     * 外键关联列名称
     */
    private String foreignColumn;

    public Column() {}

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = Objects.requireNonNull(tableName);
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = Objects.requireNonNull(columnName);
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = Objects.requireNonNull(columnType);
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public String getTableDesc() {
        return tableDesc;
    }

    public void setTableDesc(String tableDesc) {
        this.tableDesc = tableDesc;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getForeignTable() {
        return foreignTable;
    }

    public void setForeignTable(String foreignTable) {
        this.foreignTable = foreignTable;
    }

    public String getForeignColumn() {
        return foreignColumn;
    }

    public void setForeignColumn(String foreignColumn) {
        this.foreignColumn = foreignColumn;
    }
}
