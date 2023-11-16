package io.github.jiashunx.sdk.sqlite3.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQLite3JdbcTemplate单元测试
 * @author jiashunx
 */
public class SQLite3JdbcTemplateTest1 {

    private SQLite3JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() {
        jdbcTemplate = new SQLite3JdbcTemplate("/app/test/sdk-sqlite3/test1.db");
    }

    @Test
    public void test_isTableExists() {
        Assert.assertFalse(jdbcTemplate.isTableExists("table_name_undefined"));
    }

}
