package io.github.jiashunx.sdk.sqlite3.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQLite3JdbcTemplate单元测试（测试表、索引、视图、触发器等数据结构DDL）
 * @author jiashunx
 */
public class SQLite3JdbcTemplateTest1 {

    private SQLite3JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() {
        jdbcTemplate = new SQLite3JdbcTemplate("/app/test/sdk-sqlite3/test1.db");
    }

    @Test
    public void test_table() {
        jdbcTemplate.dropTableIfExists("AAA");
        jdbcTemplate.executeUpdate("create table AAA(field_1 varchar(20) not null, field_2 int4)");
        Assert.assertTrue(jdbcTemplate.isTableExists("AAA"));
        Assert.assertTrue(jdbcTemplate.isTableColumnExists("AAA", "field_1"));
        jdbcTemplate.executeUpdate("alter table AAA add column field_3 int4");
        Assert.assertTrue(jdbcTemplate.isTableColumnExists("AAA", "field_3"));
        // SQLite3不支持删除列（因此删除以下TestCase）
        // jdbcTemplate.dropTableColumn("AAA", "field_1");
        // Assert.assertFalse(jdbcTemplate.isTableColumnExists("AAA", "field_3"));
    }

    @Test
    public void test_index() {
        jdbcTemplate.dropIndexIfExists("idx_BBB");
        jdbcTemplate.dropTableIfExists("BBB");
        jdbcTemplate.executeUpdate("create table BBB(field_1 varchar(20) not null, field_2 int4)");
        Assert.assertFalse(jdbcTemplate.isIndexExists("idx_BBB"));
        jdbcTemplate.executeUpdate("create index idx_BBB on BBB(field_1)");
        Assert.assertTrue(jdbcTemplate.isIndexExists("idx_BBB"));
    }

    @Test
    public void test_view() {
        jdbcTemplate.dropViewIfExists("view_CCC");
        jdbcTemplate.dropTableIfExists("CCC");
        jdbcTemplate.executeUpdate("create table CCC(field_1 varchar(20) not null, field_2 int4)");
        Assert.assertFalse(jdbcTemplate.isViewExists("view_CCC"));
        jdbcTemplate.executeUpdate("create view view_CCC as select * from view_CCC where field_2 > 10");
        Assert.assertTrue(jdbcTemplate.isViewExists("view_CCC"));
    }

    @Test
    public void test_trigger() {
        jdbcTemplate.dropTriggerIfExists("trigger_DDD");
        jdbcTemplate.dropTableIfExists("DDD");
        jdbcTemplate.executeUpdate("create table DDD(field_1 varchar(20) not null, field_2 int4)");
        jdbcTemplate.dropTableIfExists("DDD2");
        jdbcTemplate.executeUpdate("create table DDD2(field_1 varchar(20) not null, field_2 int4, field_3 timestamp)");
        Assert.assertFalse(jdbcTemplate.isTriggerExists("trigger_DDD"));
        jdbcTemplate.executeUpdate("create trigger trigger_DDD after insert on DDD begin insert into DDD2(field_1,field_2,field_3) values (new.field_1,new.field_2,now()); end");
        Assert.assertTrue(jdbcTemplate.isTriggerExists("trigger_DDD"));
    }

}
