package io.github.jiashunx.sdk.sqlite3.metadata;

import java.util.Map;

/**
 * 查询返回结果映射类模型信息
 * @author jiashunx
 */
public class QueryRetClassModel {

    /**
     * 查询返回结果映射Class对象
     */
    private Class<?> klass;

    /**
     * 查询返回结果映射列模型信息
     */
    private Map<String, QueryRetColumnModel> retColumnModelMap;

    public Class<?> getKlass() {
        return klass;
    }

    public void setKlass(Class<?> klass) {
        this.klass = klass;
    }

    public Map<String, QueryRetColumnModel> getRetColumnModelMap() {
        return retColumnModelMap;
    }

    public void setRetColumnModelMap(Map<String, QueryRetColumnModel> retColumnModelMap) {
        this.retColumnModelMap = retColumnModelMap;
    }
}
