package io.github.jiashunx.sdk.sqlite3.metadata.xml;

import java.util.List;
import java.util.Objects;

/**
 * XML索引模型
 * @author jiashunx
 */
public class Index {

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 索引名称
     */
    private String indexName;

    /**
     * 是否唯一索引
     */
    private boolean unique;

    /**
     * 索引列字段名称
     */
    private List<String> columnNames;

    public Index() {}

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = Objects.requireNonNull(tableName);
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = Objects.requireNonNull(columnNames);
    }
}
