package io.github.jiashunx.sdk.sqlite3.metadata.xml;

import java.util.Objects;

/**
 * XML SQL模型
 * @author jiashunx
 */
public class SQL {

    /**
     * sql id（唯一标识）
     */
    private String id;

    /**
     * sql内容
     */
    private String content;

    /**
     * sql描述
     */
    private String desc;

    /**
     * 映射类名称
     */
    private String className;

    public SQL() {}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = Objects.requireNonNull(content);
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }
}
