package io.github.jiashunx.sdk.sqlite3.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * SQLite3JdbcTemplate单元测试（并发测试）
 * @author jiashunx
 */
public class SQLite3JdbcTemplateTest3 {

    private static final Logger logger = LoggerFactory.getLogger(SQLite3JdbcTemplateTest3.class);

    private SQLite3JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() {
        jdbcTemplate = new SQLite3JdbcTemplate("/app/test/sdk-sqlite3/test3.db");
    }

    @Test
    public void test_concurrency() throws InterruptedException {
        jdbcTemplate.dropTableIfExists("AAA");
        jdbcTemplate.dropTableIfExists("BBB");
        jdbcTemplate.executeUpdate("create table AAA(field_1 varchar(20) not null, field_2 int4)");
        jdbcTemplate.executeUpdate("create table BBB(field_1 varchar(20) not null, field_2 int4)");

        List<Thread> threadList = new ArrayList<>();

        // 表AAA读，10个线程
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int a = 0; a < 100; a++) {
                    int total = jdbcTemplate.queryForInt("select count(1) from AAA");
                    logger.error(Thread.currentThread().getName() + " - AAA行数: " + total);
                }
            });
            thread.setName("AAA-read-" + (i + 1));
            threadList.add(thread);
            thread.start();
        }
        // 表AAA写，5个线程（每个线程写30条）
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(() -> {
                for (int a = 0; a < 10; a++) {
                    jdbcTemplate.executeUpdate("insert into AAA(field_1,field_2) values(?,?)", statement -> {
                        statement.setString(1, UUID.randomUUID().toString());
                        statement.setInt(2, new Random().nextInt());
                    });
                    jdbcTemplate.doTransaction(() -> {
                        jdbcTemplate.executeUpdate("insert into AAA(field_1,field_2) values(?,?)", statement -> {
                            statement.setString(1, UUID.randomUUID().toString());
                            statement.setInt(2, new Random().nextInt());
                        });
                        jdbcTemplate.executeUpdate("insert into AAA(field_1,field_2) values(?,?)", statement -> {
                            statement.setString(1, UUID.randomUUID().toString());
                            statement.setInt(2, new Random().nextInt());
                        });
                    });
                }
            });
            thread.setName("AAA-write-" + (i + 1));
            threadList.add(thread);
            thread.start();
        }
        // 表BBB读，10个线程
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int a = 0; a < 100; a++) {
                    int total = jdbcTemplate.queryForInt("select count(1) from BBB");
                    logger.error(Thread.currentThread().getName() + " - BBB行数: " + total);
                }
            });
            thread.setName("BBB-read-" + (i + 1));
            threadList.add(thread);
            thread.start();
        }
        // 表BBB写，5个线程，每个线程写30条）
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(() -> {
                for (int a = 0; a < 10; a++) {
                    jdbcTemplate.executeUpdate("insert into BBB(field_1,field_2) values(?,?)", statement -> {
                        statement.setString(1, UUID.randomUUID().toString());
                        statement.setInt(2, new Random().nextInt());
                    });
                    jdbcTemplate.doTransaction(() -> {
                        jdbcTemplate.executeUpdate("insert into BBB(field_1,field_2) values(?,?)", statement -> {
                            statement.setString(1, UUID.randomUUID().toString());
                            statement.setInt(2, new Random().nextInt());
                        });
                        jdbcTemplate.executeUpdate("insert into BBB(field_1,field_2) values(?,?)", statement -> {
                            statement.setString(1, UUID.randomUUID().toString());
                            statement.setInt(2, new Random().nextInt());
                        });
                    });
                }
            });
            thread.setName("BBB-write-" + (i + 1));
            threadList.add(thread);
            thread.start();
        }
        for (Thread thread: threadList) {
            thread.join();
        }

        // 最终结果
        Assert.assertEquals(150, jdbcTemplate.queryForInt("select count(1) from AAA"));
        Assert.assertEquals(150, jdbcTemplate.queryForInt("select count(1) from BBB"));
    }

}
