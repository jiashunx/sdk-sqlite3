package io.github.jiashunx.sdk.sqlite3.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * SQLite3JdbcTemplate单元测试（测试查询、更新、批量更新等场景）
 * @author jiashunx
 */
public class SQLite3JdbcTemplateTest2 {

    private SQLite3JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() {
        jdbcTemplate = new SQLite3JdbcTemplate("/app/test/sdk-sqlite3/test2.db");
    }

    @Test
    public void test_update() {
        jdbcTemplate.dropTableIfExists("AAA");
        jdbcTemplate.executeUpdate("create table AAA(field_1 varchar(20) not null, field_2 int4)");
        jdbcTemplate.executeUpdate("insert into AAA(field_1,field_2) values('xyz',1)");
        jdbcTemplate.executeUpdate("insert into AAA(field_1,field_2) values(?,?)", statement -> {
            statement.setString(1, "xyz");
            statement.setInt(2, 10);
        });
        List<Map<String, Object>> rowObjList = jdbcTemplate.queryForList("select * from AAA order by field_2 desc");
        Assert.assertEquals(2, rowObjList.size());
        Assert.assertEquals(10, rowObjList.get(0).get("field_2"));
        Assert.assertEquals(1, rowObjList.get(1).get("field_2"));
        Map<String, Object> rowObj = jdbcTemplate.queryForMap("select * from AAA where field_2=?", statement -> {
            statement.setInt(1, 10);
        });
        Assert.assertNotNull(rowObj);
    }

    @Test
    public void test_batchUpdate_1() {
        jdbcTemplate.dropTableIfExists("BBB");
        jdbcTemplate.executeUpdate("create table BBB(field_1 varchar(20) not null, field_2 int4)");
        jdbcTemplate.executeUpdate("insert into BBB(field_1,field_2) values('xyz',1)");
        int[] resultArr = jdbcTemplate.batchUpdate(new String[]{
                "insert into BBB(field_1,field_2) values('abc',11)",
                "insert into BBB(field_1,field_2) values(?,?)",
                "update BBB set field_2=? where field_1=?"
        }, (index, statement) -> {
            if (index == 0) {
                // do nothing
            }
            if (index == 1) {
                statement.setString(1, "hello");
                statement.setInt(2, 90);
            }
            if (index == 2) {
                statement.setInt(1, 10002);
                statement.setString(2, "xyz");
            }
        });
        Assert.assertEquals(3, resultArr.length);
        Assert.assertEquals(1, resultArr[0]);
        Assert.assertEquals(1, resultArr[1]);
        Assert.assertEquals(1, resultArr[2]);
    }

    @Test
    public void test_batchUpdate_2() {
        jdbcTemplate.dropTableIfExists("CCC");
        jdbcTemplate.executeUpdate("create table CCC(field_1 varchar(20) not null, field_2 int4)");
        jdbcTemplate.batchUpdate("insert into CCC(field_1,field_2) values(?,?)", 10, (index, statement) -> {
            statement.setString(1, "string_" + index);
            statement.setInt(2, index + 100);
        });
        List<Map<String, Object>> rowObjList = jdbcTemplate.queryForList("select * from CCC order by field_2 desc");
        Assert.assertEquals(10, rowObjList.size());
        Assert.assertEquals("string_9", rowObjList.get(0).get("field_1"));
        Assert.assertEquals(109, rowObjList.get(0).get("field_2"));
    }

    @Test
    public void test_doTransaction() {
        jdbcTemplate.dropTableIfExists("DDD");
        jdbcTemplate.executeUpdate("create table DDD(field_1 varchar(20) not null, field_2 int4)");
        jdbcTemplate.doTransaction(() -> {
            // batchUpdate_1
            jdbcTemplate.executeUpdate("insert into DDD(field_1,field_2) values('xyz',1)");
            jdbcTemplate.batchUpdate(new String[]{
                    "insert into DDD(field_1,field_2) values('abc',11)",
                    "insert into DDD(field_1,field_2) values(?,?)",
                    "update DDD set field_2=? where field_1=?"
            }, (index, statement) -> {
                if (index == 0) {
                    // do nothing
                }
                if (index == 1) {
                    statement.setString(1, "hello");
                    statement.setInt(2, 90);
                }
                if (index == 2) {
                    statement.setInt(1, 10002);
                    statement.setString(2, "xyz");
                }
            });
            // batchUpdate_2
            jdbcTemplate.batchUpdate("insert into DDD(field_1,field_2) values(?,?)", 10, (index, statement) -> {
                statement.setString(1, "string_" + index);
                statement.setInt(2, index + 100);
            });
        });
        List<Map<String, Object>> rowObjList = jdbcTemplate.queryForList("select * from DDD order by field_2 desc");
        Assert.assertEquals(13, rowObjList.size());
    }

}
