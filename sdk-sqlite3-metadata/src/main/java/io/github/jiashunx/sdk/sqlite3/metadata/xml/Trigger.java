package io.github.jiashunx.sdk.sqlite3.metadata.xml;

/**
 * XML触发器模型
 * @author jiashunx
 */
public class Trigger {

    /**
     * 触发器名称
     */
    private String triggerName;

    /**
     * 触发器定义SQL
     */
    private String triggerSQL;

    /**
     * 触发器描述信息
     */
    private String description;

    public String getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public String getTriggerSQL() {
        return triggerSQL;
    }

    public void setTriggerSQL(String triggerSQL) {
        this.triggerSQL = triggerSQL;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
