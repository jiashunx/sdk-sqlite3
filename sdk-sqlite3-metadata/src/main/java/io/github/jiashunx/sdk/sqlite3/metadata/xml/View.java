package io.github.jiashunx.sdk.sqlite3.metadata.xml;

import java.util.Objects;

/**
 * XML视图模型
 * @author jiashunx
 */
public class View {

    /**
     * 视图名称
     */
    private String viewName;

    /**
     * 是否临时视图
     */
    private boolean temporary;

    /**
     * 视图定义SQL
     */
    private String content;

    /**
     * 视图描述信息
     */
    private String viewDesc;

    public View() {}

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = Objects.requireNonNull(viewName);
    }

    public boolean isTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = Objects.requireNonNull(content);
    }

    public String getViewDesc() {
        return viewDesc;
    }

    public void setViewDesc(String viewDesc) {
        this.viewDesc = viewDesc;
    }
}
