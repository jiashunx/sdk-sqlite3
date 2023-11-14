package io.github.jiashunx.sdk.sqlite3.metadata;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 查询返回结果
 * @author jiashunx
 */
public class QueryResult {

    /**
     * 查询返回结果列字段名称与列字段元数据信息映射map
     */
    private final Map<String, ColumnMetadata> columnMetadataMap;

    /**
     * 查询返回结果List
     */
    private final List<Map<String, Object>> retMapList;

    public QueryResult(Map<String, ColumnMetadata> columnMetadataMap, List<Map<String, Object>> retMapList) {
        this.columnMetadataMap = Objects.requireNonNull(columnMetadataMap);
        this.retMapList = retMapList;
    }

    public Map<String, ColumnMetadata> getColumnMetadataMap() {
        return columnMetadataMap;
    }

    public List<Map<String, Object>> getRetMapList() {
        return retMapList;
    }

}
