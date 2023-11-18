package io.github.jiashunx.sdk.sqlite3.metadata;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 数据表模型信息
 * @author jiashunx
 */
public class TableModel {

    /**
     * class对象.
     */
    private Class<?> klass;

    /**
     * class名称.
     */
    private String klassName;

    /**
     * table名称.
     */
    private String tableName;

    /**
     * table主键字段名称对应的table字段模型.
     */
    private TableColumnModel idColumnModel;

    /**
     * table字段名称与对应table字段模型的映射
     */
    private Map<String, TableColumnModel> columnModelMap;

    /**
     * table字段模型列表(按照搜索顺序顺序排列, id字段放至最后).
     */
    private List<TableColumnModel> columnModelList;

    /**
     * 数据表字段定义信息.
     */
    private Map<String, ColumnMetadata> columnMetadata;

    /**
     * 获取单条插入SQL（包括所有实体字段）
     * @return 单条插入SQL
     */
    public String sqlOfInsert() {
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        builder.append(tableName).append("(");
        columnModelList.forEach(columnModel -> {
            builder.append(columnModel.getColumnName()).append(",");
        });
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") VALUES(");
        columnModelList.forEach(columnModel -> {
            builder.append("?,");
        });
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        return builder.toString();
    }

    /**
     * 获取单条更新SQL（根据主键更新）（包括所有实体字段）
     * @return 单条更新SQL
     */
    public String sqlOfUpdate() {
        StringBuilder builder = new StringBuilder("UPDATE ");
        builder.append(tableName).append(" SET ");
        columnModelList.forEach(columnModel -> {
            if (!columnModel.isIdColumn()) {
                builder.append(columnModel.getColumnName()).append("=?,");
            }
        });
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" WHERE ");
        builder.append(idColumnModel.getColumnName()).append("=?");
        return builder.toString();
    }

    /**
     * 获取删除SQL（根据主键删除）
     * @return 删除SQL（根据主键删除）
     */
    public String sqlOfDeleteById() {
        return sqlOfDelete(builder -> {
            builder.append(" WHERE ").append(idColumnModel.getColumnName()).append("=? ");
        });
    }

    /**
     * 获取删除SQL（自行拼接删除条件）
     * @param consumer 拼接删除条件
     * @return 删除SQL（根据主键删除）
     */
    public String sqlOfDelete(Consumer<StringBuilder> consumer) {
        StringBuilder builder = new StringBuilder("DELETE FROM " + tableName + " ");
        consumer.accept(builder);
        return builder.toString();
    }

    /**
     * 获取查询SQL（查询所有）
     * @return 查询SQL（查询所有）
     */
    public String sqlOfSelectAll() {
        return sqlOfSelectAll(builder -> {});
    }

    /**
     * 获取查询SQL（自行拼接查询条件）
     * @param consumer 拼接查询条件
     * @return 查询SQL（查询所有）
     */
    public String sqlOfSelectAll(Consumer<StringBuilder> consumer) {
        StringBuilder builder = new StringBuilder("SELECT * FROM " + tableName + " ");
        consumer.accept(builder);
        return builder.toString();
    }

    /**
     * 获取查询SQL（根据主键查询）
     * @return 查询SQL（根据主键查询）
     */
    public String sqlOfSelectById() {
        return sqlOfSelect(builder -> {
            builder.append(" WHERE ").append(idColumnModel.getColumnName()).append("=? ");
        });
    }

    /**
     * 获取查询SQL（自行拼接查询条件）
     * @param consumer 拼接查询条件
     * @return 查询SQL（自行拼接查询条件）
     */
    public String sqlOfSelect(Consumer<StringBuilder> consumer) {
        return sqlOfSelectAll(consumer);
    }

    /**
     * 创建表模型实例
     * @return 表模型实例
     */
    public Object newInstance() {
        try {
            return klass.newInstance();
        } catch (Throwable throwable) {
            throw new RuntimeException(String.format("create class [%s] instance failed.", klassName), throwable);
        }
    }

    /**
     * 获取表模型实例主键字段值
     * @param object 表模型实例
     * @return 主键字段值
     */
    public Object getIdFieldValue(Object object) {
        Field idField = idColumnModel.getField();
        try {
            idField.setAccessible(true);
            return idField.get(object);
        } catch (Throwable throwable) {
            throw new RuntimeException(String.format("get id field[%s] value faild.", idField), throwable);
        }
    }

    public Class<?> getKlass() {
        return klass;
    }

    public void setKlass(Class<?> klass) {
        this.klass = klass;
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

    public TableColumnModel getIdColumnModel() {
        return idColumnModel;
    }

    public void setIdColumnModel(TableColumnModel idColumnModel) {
        this.idColumnModel = idColumnModel;
    }

    public Map<String, TableColumnModel> getColumnModelMap() {
        return columnModelMap;
    }

    public void setColumnModelMap(Map<String, TableColumnModel> columnModelMap) {
        this.columnModelMap = columnModelMap;
    }

    public List<TableColumnModel> getColumnModelList() {
        return columnModelList;
    }

    public void setColumnModelList(List<TableColumnModel> columnModelList) {
        this.columnModelList = columnModelList;
    }

    public Map<String, ColumnMetadata> getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(Map<String, ColumnMetadata> columnMetadata) {
        this.columnMetadata = columnMetadata;
    }
}
