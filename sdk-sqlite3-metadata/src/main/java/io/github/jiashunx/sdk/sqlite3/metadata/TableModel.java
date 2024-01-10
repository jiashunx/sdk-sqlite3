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
     * @return 单条插入SQL: insert into table_name(field1,field2,field3,...) values(?,?,?,...)
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
     * @return 单条更新SQL: update table_name set field1=?,field2=?,field3=?,... where id=?
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
     * @return 删除SQL（根据主键删除）: delete from table_name where id=?
     */
    public String sqlOfDeleteById() {
        return sqlOfDelete(builder -> {
            builder.append(" WHERE ").append(idColumnModel.getColumnName()).append("=? ");
        });
    }

    /**
     * 获取删除SQL（自行拼接删除条件）
     * @param consumer 拼接删除条件: 拼接: where field1=? and field2=?
     * @return 删除SQL（根据主键删除）: delete from table_name where field1=? and field2=?
     */
    public String sqlOfDelete(Consumer<StringBuilder> consumer) {
        StringBuilder builder = new StringBuilder("DELETE FROM " + tableName + " ");
        consumer.accept(builder);
        return builder.toString();
    }

    /**
     * 获取查询SQL（查询所有）
     * @return 查询SQL（查询所有）: select * from table_name
     */
    public String sqlOfSelectAll() {
        return sqlOfSelectAll(builder -> {});
    }

    /**
     * 获取查询SQL（自行拼接查询条件）
     * @param consumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @return 查询SQL（查询所有）: select * from table_name where field1=? and field2=?
     */
    public String sqlOfSelectAll(Consumer<StringBuilder> consumer) {
        StringBuilder builder = new StringBuilder("SELECT * FROM " + tableName + " ");
        consumer.accept(builder);
        return builder.toString();
    }

    /**
     * 获取查询SQL（查询所有）
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 查询SQL（查询所有）: select * from table_name
     */
    public String sqlOfSelectAllWithPage(int pageIndex, int pageSize) {
        return sqlOfSelectAllWithPage(builder -> {}, pageIndex, pageSize);
    }

    /**
     * 获取查询SQL（自行拼接查询条件）
     * @param consumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 查询SQL（查询所有）: select * from table_name where field1=? and field2=?
     */
    public String sqlOfSelectAllWithPage(Consumer<StringBuilder> consumer, int pageIndex, int pageSize) {
        return sqlOfSelectAll(consumer) + String.format(" LIMIT %d,%d ", (pageIndex - 1) * pageSize, pageSize);
    }

    /**
     * 获取查询SQL（查询指定字段）
     * @param fieldNames 待查询字段名称列表
     * @return 查询SQL（查询指定字段）: select field1,field2,field3 from table_name
     */
    public String sqlOfSelectFields(List<String> fieldNames) {
        return sqlOfSelectFields(fieldNames, builder -> {});
    }

    /**
     * 获取查询SQL（查询指定字段）
     * @param fieldNames 待查询字段名称列表, 若为空则查询所有字段
     * @param consumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @return 查询SQL（查询指定字段）: select field1,field2,field3 from table_name where field1=? and field2=?
     */
    public String sqlOfSelectFields(List<String> fieldNames, Consumer<StringBuilder> consumer) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return sqlOfSelect(consumer);
        }
        StringBuilder builder = new StringBuilder("SELECT ");
        for (int i = 0, size = fieldNames.size(); i < size; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(fieldNames.get(i));
        }
        builder.append(" FROM ").append(tableName).append(" ");
        consumer.accept(builder);
        return builder.toString();
    }

    /**
     * 获取查询SQL（查询指定字段）
     * @param fieldNames 待查询字段名称列表
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 查询SQL（查询指定字段）: select field1,field2,field3 from table_name
     */
    public String sqlOfSelectFieldsWithPage(List<String> fieldNames, int pageIndex, int pageSize) {
        return sqlOfSelectFieldsWithPage(fieldNames, builder -> {}, pageIndex, pageSize);
    }

    /**
     * 获取查询SQL（查询指定字段）
     * @param fieldNames 待查询字段名称列表, 若为空则查询所有字段
     * @param consumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 查询SQL（查询指定字段）: select field1,field2,field3 from table_name where field1=? and field2=?
     */
    public String sqlOfSelectFieldsWithPage(List<String> fieldNames, Consumer<StringBuilder> consumer, int pageIndex, int pageSize) {
        return sqlOfSelectFields(fieldNames, consumer) + String.format(" LIMIT %d,%d ", (pageIndex - 1) * pageSize, pageSize);
    }

    /**
     * 获取查询SQL（根据主键查询）
     * @return 查询SQL（根据主键查询）: select * from table_name where id=?
     */
    public String sqlOfSelectById() {
        return sqlOfSelect(builder -> {
            builder.append(" WHERE ").append(idColumnModel.getColumnName()).append("=? ");
        });
    }

    /**
     * 获取查询SQL（自行拼接查询条件）
     * @param consumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @return 查询SQL（自行拼接查询条件）: select * from table_name where field1=? and field2=?
     */
    public String sqlOfSelect(Consumer<StringBuilder> consumer) {
        return sqlOfSelectAll(consumer);
    }

    /**
     * 获取查询SQL（自行拼接查询条件）
     * @param consumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 查询SQL（自行拼接查询条件）: select * from table_name where field1=? and field2=?
     */
    public String sqlOfSelectWithPage(Consumer<StringBuilder> consumer, int pageIndex, int pageSize) {
        return sqlOfSelect(consumer) + String.format(" LIMIT %d,%d ", (pageIndex - 1) * pageSize, pageSize);
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
