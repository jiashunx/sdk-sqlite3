package io.github.jiashunx.sdk.sqlite3.mapping.util;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3SqlException;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.Column;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.Index;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.SQL;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.SQLPackage;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.Trigger;
import io.github.jiashunx.sdk.sqlite3.metadata.xml.View;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SQL解析工具
 * @author jiashunx
 */
public class SQLite3SQLHelper {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3SQLHelper.class);

    /**
     * 私有构造方法
     */
    private SQLite3SQLHelper() {}

    /**
     * 从classpath加载SQL包
     * @param filePath classpath路径
     * @return SQL包
     * @throws SQLite3SqlException SQLite3SqlException
     */
    public static SQLPackage loadSQLPackageFromClasspath(String filePath) throws SQLite3SqlException {
        return loadSQLPackageFromClasspath(filePath, SQLite3SQLHelper.class.getClassLoader());
    }

    /**
     * 从classpath加载SQL包
     * @param filePath classpath路径
     * @param classLoader 类加载器
     * @return SQL包
     * @throws SQLite3SqlException SQLite3SqlException
     */
    public static SQLPackage loadSQLPackageFromClasspath(String filePath, ClassLoader classLoader) throws SQLite3SqlException {
        try {
            return loadSQLPackage(classLoader.getResourceAsStream(filePath));
        } catch (Throwable throwable) {
            throw new SQLite3SqlException(String.format("load SQLPackage from classpath: %s failed, classLoader: %s", filePath, String.valueOf(classLoader)));
        }
    }

    /**
     * 从本地加载SQL包
     * @param filePath 本地路径
     * @return SQL包
     * @throws SQLite3SqlException SQLite3SqlException
     */
    public static SQLPackage loadSQLPackageFromDisk(String filePath) throws SQLite3SqlException {
        try {
            return loadSQLPackage(new FileInputStream(filePath));
        } catch (Throwable throwable) {
            throw new SQLite3SqlException(String.format("load SQLPackage from disk: %s failed", filePath));
        }
    }

    /**
     * 从输入流加载SQL包
     * @param inputStream 输入流
     * @return SQL包
     * @throws SQLite3SqlException SQLite3SqlException
     */
    public static SQLPackage loadSQLPackage(InputStream inputStream) throws SQLite3SqlException {
        SQLPackage sqlPackage = null;
        try {
            Element rootElement = new SAXReader().read(inputStream).getRootElement();
            sqlPackage = new SQLPackage();
            sqlPackage.setGroupId(rootElement.attributeValue("id"));
            sqlPackage.setGroupName(rootElement.attributeValue("name"));
            AtomicReference<SQLPackage> sqlPackageRef = new AtomicReference<>(sqlPackage);
            // DQL
            Optional.ofNullable(rootElement.elements("dql")).ifPresent(dqlElements -> {
                dqlElements.forEach(dqlElement -> {
                    Optional.ofNullable(dqlElement.elements("sql")).ifPresent(sqlElements -> {
                        sqlElements.forEach(sqlElement -> {
                            SQL sql = new SQL();
                            sql.setId(sqlElement.attributeValue("id"));
                            sql.setDesc(sqlElement.attributeValue("desc"));
                            sql.setClassName(sqlElement.attributeValue("class"));
                            sql.setContent(sqlElement.getText().replace("    ", "").replace("\n", " "));
                            sqlPackageRef.get().addDQL(sql);
                        });
                    });
                });
            });
            // DML
            Optional.ofNullable(rootElement.elements("dml")).ifPresent(dmlElements -> {
                dmlElements.forEach(dmlElement -> {
                    Optional.ofNullable(dmlElement.elements("sql")).ifPresent(sqlElements -> {
                        sqlElements.forEach(sqlElement -> {
                            SQL sql = new SQL();
                            sql.setId(sqlElement.attributeValue("id"));
                            sql.setDesc(sqlElement.attributeValue("desc"));
                            sql.setContent(sqlElement.getText().replace("    ", "").replace("\n", " "));
                            sqlPackageRef.get().addDML(sql);
                        });
                    });
                });
            });
            // DDL
            Optional.ofNullable(rootElement.elements("ddl")).ifPresent(ddlElements -> {
                ddlElements.forEach(ddlElement -> {
                    // TABLE
                    Optional.ofNullable(ddlElement.elements("table")).ifPresent(tableElements -> {
                        tableElements.forEach(tableElement -> {
                            String tableName = tableElement.attributeValue("name");
                            String tableDesc = tableElement.attributeValue("desc");
                            Optional.ofNullable(tableElement.elements("column")).ifPresent(columnElements -> {
                                columnElements.forEach(columnElement -> {
                                    Column column =  new Column();
                                    column.setColumnName(columnElement.attributeValue("name"));
                                    column.setColumnType(columnElement.attributeValue("type"));
                                    column.setPrimary("true".equals(columnElement.attributeValue("primary")));
                                    column.setColumnComment(columnElement.attributeValue("comment"));
                                    String length = columnElement.attributeValue("length");
                                    column.setLength(Integer.MIN_VALUE);
                                    if (length != null && !"".equals(length.trim())) {
                                        column.setLength(Integer.parseInt(length.trim()));
                                    }
                                    column.setNotNull("true".equals(columnElement.attributeValue("not-null")));
                                    String defaultValue = columnElement.attributeValue("default");
                                    if (defaultValue == null || defaultValue.trim().isEmpty()) {
                                        defaultValue = "";
                                    }
                                    column.setDefaultValue(defaultValue);
                                    column.setTableName(tableName);
                                    column.setTableDesc(tableDesc);
                                    column.setForeignTable(columnElement.attributeValue("foreign-table"));
                                    column.setForeignColumn(columnElement.attributeValue("foreign-column"));
                                    sqlPackageRef.get().addColumnDDL(column);
                                });
                            });
                        });
                    });
                    // VIEW
                    Optional.ofNullable(ddlElement.elements("view")).ifPresent(viewElements -> {
                        viewElements.forEach(viewElement -> {
                            View view = new View();
                            view.setViewName(viewElement.attributeValue("name"));
                            view.setViewDesc(viewElement.attributeValue("desc"));
                            view.setTemporary("true".equals(viewElement.attributeValue("temporary")));
                            view.setContent(viewElement.getText().replace("    ", "").replace("\n", " "));
                            sqlPackageRef.get().addViewDDL(view);
                        });
                    });
                    // INDEX
                    Optional.ofNullable(ddlElement.elements("index")).ifPresent(indexElements -> {
                        indexElements.forEach(indexElement -> {
                            Index index = new Index();
                            index.setIndexName(indexElement.attributeValue("name"));
                            index.setTableName(indexElement.attributeValue("table"));
                            index.setUnique("true".equals(indexElement.attributeValue("unique")));
                            AtomicReference<List<String>> columnNamesRef = new AtomicReference<List<String>>(new ArrayList<>());
                            Optional.ofNullable(indexElement.elements("column")).ifPresent(columnElements -> {
                                columnElements.forEach(columnElement -> {
                                    String columnName = columnElement.attributeValue("name");
                                    columnNamesRef.get().add(columnName);
                                });
                            });
                            index.setColumnNames(columnNamesRef.get());
                            sqlPackageRef.get().addIndexDDL(index);
                        });
                    });
                    // TRIGGER
                    Optional.ofNullable(ddlElement.elements("trigger")).ifPresent(triggerElements -> {
                        triggerElements.forEach(triggerElement -> {
                            Trigger trigger = new Trigger();
                            trigger.setTriggerName(triggerElement.attributeValue("name"));
                            trigger.setTriggerSQL(triggerElement.getText().replace("    ", " ").replace("\n", " "));
                            trigger.setDescription(triggerElement.attributeValue("desc"));
                            sqlPackageRef.get().addTriggerDDL(trigger);
                        });
                    });
                });
            });
        } catch (Throwable throwable) {
            throw new SQLite3SqlException("load SQLPackage from InputStream failed.", throwable);
        }
        return sqlPackage;
    }

}
