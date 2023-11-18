package io.github.jiashunx.sdk.sqlite3.metadata.xml;

import org.sqlite.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * XML SQL包模型
 * @author jiashunx
 */
public class SQLPackage {

    /**
     * groupId.
     */
    private String groupId;

    /**
     * groupName.
     */
    private String groupName;

    /**
     * key - sqlid
     */
    private final Map<String, SQL> dql = new LinkedHashMap<>();

    /**
     * key - sqlid
     */
    private final Map<String, SQL> dml = new LinkedHashMap<>();

    /**
     * key - tableName
     */
    private final Map<String, List<Column>> columnDDL = new LinkedHashMap<>();

    /**
     * key - tableName_columnName
     */
    private final Map<String, Column> columnMap = new LinkedHashMap<>();

    /**
     * key - tableName
     */
    private final Map<String, List<Index>> indexDDL = new LinkedHashMap<>();

    /**
     * key - tableName_columnName
     */
    private final Map<String, Index> indexMap = new LinkedHashMap<>();

    /**
     * key - viewName
     */
    private final Map<String, View> viewDDL = new LinkedHashMap<>();

    /**
     * key - triggerName
     */
    private final Map<String, Trigger> triggerDDL = new LinkedHashMap<>();

    public SQLPackage() {}

    /**
     * 获取SQL包下的表名称列表
     * @return 表名称列表
     */
    public List<String> getTableNames() {
        return new ArrayList<>(columnDDL.keySet());
    }

    /**
     * 获取SQL包下的索引名称列表
     * @return 索引名称列表
     */
    public List<String> getIndexNames() {
        return new ArrayList<>(indexDDL.keySet());
    }

    /**
     * 获取SQL包下的视图名称列表
     * @return 视图名称列表
     */
    public List<String> getViewNames() {
        return new ArrayList<>(viewDDL.keySet());
    }

    /**
     * 获取SQL包下的触发器名称列表
     * @return 触发器名称列表
     */
    public List<String> getTriggerNames() {
        return new ArrayList<>(triggerDDL.keySet());
    }

    /**
     * 获取SQL包下指定表DDL语句
     * @param tableName 表名称
     * @return 表DDL语句
     */
    public String getTableDefineSQL(String tableName) {
        List<Column> columns = getColumns(tableName);
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder("CREATE TABLE ").append(tableName).append("(");
        List<String> primaryColumns = new ArrayList<>();
        List<String> columnDefList = new ArrayList<>();
        List<String> foreignKeyDefList = new ArrayList<>();
        columns.forEach(column -> {
            if (column.isPrimary()) {
                primaryColumns.add(column.getColumnName());
            }
            String lengthStr = "";
            if (column.getLength() != Integer.MIN_VALUE){
                lengthStr = "(" + column.getLength() + ")";
            }
            String notNull = "";
            if (column.isNotNull()) {
                notNull = " NOT NULL ";
            }
            String defaultValue = "";
            if (!column.getDefaultValue().isEmpty()) {
                defaultValue = " DEFAULT " + column.getDefaultValue();
            }
            columnDefList.add(column.getColumnName() + " " + column.getColumnType() + lengthStr + notNull + defaultValue);
            if (column.getForeignTable() != null && column.getForeignColumn() != null
                    && !column.getForeignTable().trim().isEmpty() && !column.getForeignColumn().trim().isEmpty()) {
                foreignKeyDefList.add("FOREIGN KEY (" + column.getColumnName() + ") REFERENCES " + column.getForeignTable().trim() + "(" + column.getForeignColumn().trim() + ")");
            }
        });
        builder.append(StringUtils.join(columnDefList, ","));
        if (!primaryColumns.isEmpty()) {
            builder.append(",")
                    .append(" PRIMARY KEY(")
                    .append(StringUtils.join(primaryColumns, ","))
                    .append(")");
        }
        if (!foreignKeyDefList.isEmpty()) {
            builder.append(",")
                    .append(StringUtils.join(foreignKeyDefList, ","));
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * 获取SQL包下指定索引DDL语句
     * @param viewName 索引名称
     * @return 索引DDL语句
     */
    public String getViewDefineSQL(String viewName) {
        View view = getView(viewName);
        if (view == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder("CREATE ");
        if (view.isTemporary()) {
            builder.append(" TEMPORARY ");
        }
        builder.append(" VIEW ");
        builder.append(viewName).append(" AS ").append(view.getContent());
        return builder.toString();
    }

    /**
     * 获取SQL包下指定表中列DDL语句
     * @param tableName 表名称
     * @param columnName 列名称
     * @return 列DDL语句
     */
    public String getColumnDefineSQL(String tableName, String columnName) {
        Column column = getColumn(tableName, columnName);
        if (column == null) {
            return null;
        }
        String lengthStr = "";
        if (column.getLength() != Integer.MIN_VALUE){
            lengthStr = "(" + column.getLength() + ")";
        }
        String notNull = "";
        if (column.isNotNull()) {
            notNull = " NOT NULL ";
        }
        String defaultValue = "";
        if (!column.getDefaultValue().isEmpty()) {
            defaultValue = " DEFAULT " + column.getDefaultValue();
        }
        return "ALTER TABLE " + tableName + " ADD COLUMN " +
                columnName +
                " " +
                column.getColumnType() + lengthStr + notNull + defaultValue;
    }

    /**
     * 获取SQL包下指定表索引DDL语句
     * @param tableName 表名称
     * @param indexName 索引名称
     * @return 表索引DDL语句
     */
    public String getIndexDefineSQL(String tableName, String indexName) {
        Index index = getIndex(tableName, indexName);
        if (index == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder("CREATE ");
        if (index.isUnique()) {
            builder.append(" UNIQUE ");
        }
        builder.append(" INDEX ")
                .append(indexName).append(" ON ").append(tableName)
                .append("(")
                .append(StringUtils.join(index.getColumnNames(), ","))
                .append(")");
        return builder.toString();
    }

    public String getTriggerDefineSQL(String triggerName) {
        Trigger trigger = getTrigger(triggerName);
        if (trigger == null) {
            return null;
        }
        return trigger.getTriggerSQL();
    }

    /**
     * 获取DQL
     * @param sqlId sqlId
     * @return DQL
     */
    public SQL getDQL(String sqlId) {
        return dql.get(sqlId);
    }

    /**
     * 获取DML
     * @param sqlId sqlId
     * @return DML
     */
    public SQL getDML(String sqlId) {
        return dml.get(sqlId);
    }

    /**
     * 获取数据表列定义列表
     * @param tableName 表名称
     * @return 列定义列表
     */
    public List<Column> getColumns(String tableName) {
        return columnDDL.get(tableName);
    }

    /**
     * 获取数据表列定义
     * @param tableName 表名称
     * @param columnName 列名称
     * @return 列定义
     */
    public Column getColumn(String tableName, String columnName) {
        return columnMap.get(tableName + "_" + columnName);
    }

    /**
     * 获取数据表索引定义列表
     * @param tableName 表名称
     * @return 索引定义列表
     */
    public List<Index> getIndexes(String tableName) {
        return indexDDL.get(tableName);
    }

    /**
     * 获取数据表索引定义
     * @param tableName 表名称
     * @param indexName 索引名称
     * @return 索引定义
     */
    public Index getIndex(String tableName, String indexName) {
        return indexMap.get(tableName + "_" + indexName);
    }

    /**
     * 获取视图定义
     * @param viewName 视图名称
     * @return 视图定义
     */
    public View getView(String viewName) {
        return viewDDL.get(viewName);
    }

    /**
     * 获取触发器定义
     * @param triggerName 触发器名称
     * @return 触发器定义
     */
    public Trigger getTrigger(String triggerName) {
        return triggerDDL.get(triggerName);
    }

    /**
     * 添加DQL
     * @param sql SQL
     */
    public synchronized void addDQL(SQL sql) {
        dql.put(sql.getId(), sql);
    }

    /**
     * 添加DML
     * @param sql SQL
     */
    public synchronized void addDML(SQL sql) {
        dml.put(sql.getId(), sql);
    }

    /**
     * 添加列定义
     * @param column Column
     */
    public synchronized void addColumnDDL(Column column) {
        columnDDL.computeIfAbsent(column.getTableName(), k -> new ArrayList<>()).add(column);
        columnMap.put(column.getTableName() + "_" + column.getColumnName(), column);
    }

    /**
     * 添加索引定义
     * @param index Index
     */
    public synchronized void addIndexDDL(Index index) {
        indexDDL.computeIfAbsent(index.getTableName(), k -> new ArrayList<>()).add(index);
        indexMap.put(index.getTableName() + "_" + index.getIndexName(), index);
    }

    /**
     * 添加视图定义
     * @param view View
     */
    public synchronized void addViewDDL(View view) {
        viewDDL.put(view.getViewName(), view);
    }

    /**
     * 添加触发器定义
     * @param trigger 触发器
     */
    public synchronized void addTriggerDDL(Trigger trigger) {
        triggerDDL.put(trigger.getTriggerName(), trigger);
    }

    /**
     * 获取groupId
     * @return groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * 设置groupId
     * @param groupId groupId
     */
    public void setGroupId(String groupId) {
        this.groupId = Objects.requireNonNull(groupId);
    }

    /**
     * 获取groupName
     * @return groupName
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * 设置groupName
     * @param groupName groupName
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
