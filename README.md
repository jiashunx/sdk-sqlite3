
### sdk-sqlite3

- 项目简介：Java操作SQLite3的工具包（支持多线程并发读、独占写），从 [tools-sqlite3][1] 工程迁移而来（原项目已废弃）。

- 主要功能：
   - SQLite3连接池管理，同时封装JdbcTemplate，支持多线程环境下使用SQLite3
   - 支持数据结构初始化及更新（表、索引、视图、触发器等）
   - 简易的模型映射处理实现，具体参见工程测试用例

- 支持版本：
   - JDK11+

- Maven依赖(最新版本: <b>1.1.0.RELEASE</b>)：

```text
   <dependency>
       <groupId>io.github.jiashunx</groupId>
       <artifactId>sdk-sqlite3</artifactId>
       <version>1.0.1.RELEASE</version>
   </dependency>
```

[1]: https://github.com/jiashunx/tools-sqlite3
