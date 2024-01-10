package io.github.jiashunx.sdk.sqlite3.mapping.service;

import io.github.jiashunx.sdk.sqlite3.core.exception.SQLite3Exception;
import io.github.jiashunx.sdk.sqlite3.core.function.VoidFunc;
import io.github.jiashunx.sdk.sqlite3.core.sql.SQLite3PreparedStatement;
import io.github.jiashunx.sdk.sqlite3.mapping.SQLite3JdbcTemplate;
import io.github.jiashunx.sdk.sqlite3.mapping.util.SQLite3Utils;
import io.github.jiashunx.sdk.sqlite3.metadata.TableModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * SQLite3模型映射服务（取消支持单机缓存）
 * @author jiashunx
 */
public abstract class SQLite3Service<Entity, ID> {

    /**
     * SQLite3JdbcTemplate实例
     */
    private final SQLite3JdbcTemplate jdbcTemplate;

    /**
     * 默认实体对象
     */
    private final Entity defaultEntity;

    /**
     * 是否支持单机缓存（true-支持，false-不支持）
     */
    @Deprecated
    private final boolean cacheEnabled = false;

    /**
     * listAll方式是否已执行
     */
    @Deprecated
    private volatile boolean listAllMethodInvoked = false;

    /**
     * 实体数据全局缓存
     */
    private final Map<ID, Entity> entityCacheMap = new LinkedHashMap<>();

    /**
     * 实体数据局部缓存
     */
    private final Map<ID, Entity> entityCacheMapTmp = new HashMap<>();

    /**
     * 缓存数据读写锁
     */
    private final ReentrantReadWriteLock entityCacheMapLock = new ReentrantReadWriteLock();

    /**
     * 构造方法（默认支持单机缓存）
     * @param jdbcTemplate SQLite3JdbcTemplate
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public SQLite3Service(SQLite3JdbcTemplate jdbcTemplate) throws NullPointerException, SQLite3Exception {
        // 默认开启缓存
        this(jdbcTemplate, true);
    }

    /**
     * 构造方法（自行指定是否开启缓存
     * @param jdbcTemplate SQLite3JdbcTemplate
     * @param cacheEnabled 是否支持单机缓存
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public SQLite3Service(SQLite3JdbcTemplate jdbcTemplate, boolean cacheEnabled) throws NullPointerException, SQLite3Exception {
        // this.cacheEnabled = cacheEnabled;
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        try {
            this.defaultEntity = getEntityClass().newInstance();
        } catch (Throwable throwable) {
            throw new SQLite3Exception(String.format("create entity [%s] instance failed", getEntityClass()), throwable);
        }
    }

    /**
     * 获取SQLite3JdbcTemplate实例
     * @return SQLite3JdbcTemplate
     */
    public SQLite3JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * 获取实体缓存（ID与实体对象映射map）
     * @return 实体缓存数据
     */
    @Deprecated
    private Map<ID, Entity> getEntityCacheMap() {
        if (listAllMethodInvoked) {
            return entityCacheMap;
        }
        return entityCacheMapTmp;
    }

    /**
     * 子类实现方法：获取实体类型
     * @return 实体类型
     */
    protected abstract Class<Entity> getEntityClass();

    /**
     * 实体缓存读处理
     * @param voidFunc VoidFunc
     */
    protected void entityCacheReadLock(VoidFunc voidFunc) {
        entityCacheMapLock.readLock().lock();
        try {
            voidFunc.apply();
        } finally {
            entityCacheMapLock.readLock().unlock();
        }
    }

    /**
     * 实体缓存写处理
     * @param voidFunc VoidFunc
     */
    protected void entityCacheWriteLock(VoidFunc voidFunc) {
        entityCacheMapLock.writeLock().lock();
        try {
            voidFunc.apply();
        } finally {
            entityCacheMapLock.writeLock().unlock();
        }
    }

    /**
     * 获取查询所有数据的查询SQL
     * @return 查询SQL
     */
    @Deprecated
    protected String sqlOfSelectAll() {
        return SQLite3Utils.getClassTableModel(getEntityClass()).sqlOfSelectAll();
    }

    /**
     * 从数据库查询并返回实体数据列表（所有数据）
     * @return 实体数据列表（所有数据）
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public List<Entity> listAllWithNoCache() throws NullPointerException, SQLite3Exception {
        return getJdbcTemplate().queryForList(sqlOfSelectAll(), getEntityClass());
    }

    /**
     * 从缓存或数据库查询并返回实体数据列表（所有数据）
     * @return 实体数据列表（所有数据）
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public List<Entity> listAll() throws NullPointerException, SQLite3Exception {
        if (!cacheEnabled) {
            return listAllWithNoCache();
        }
        AtomicReference<List<Entity>> ref = new AtomicReference<>();
        if (!listAllMethodInvoked) {
            entityCacheWriteLock(() -> {
                if (listAllMethodInvoked) {
                    return;
                }
                entityCacheMapTmp.clear();
                List<Entity> entityList = listAllWithNoCache();
                for (Entity entity: entityList) {
                    entityCacheMap.put(getIdFieldValue(entity), entity);
                }
                listAllMethodInvoked = true;
            });
        }
        entityCacheReadLock(() -> {
            ref.set(new ArrayList<>(entityCacheMap.values()));
        });
        return ref.get();
    }

    /**
     * 查询实体列表（查询所有字段）
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> select() throws NullPointerException, SQLite3Exception {
        return select(builder -> {}, statement -> {});
    }

    /**
     * 查询实体列表（查询所有字段）
     * @param sqlConsumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param statementConsumer sql语句预编译处理
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> select(Consumer<StringBuilder> sqlConsumer, Consumer<SQLite3PreparedStatement> statementConsumer) throws NullPointerException, SQLite3Exception {
        return getJdbcTemplate().queryForList(SQLite3Utils.getClassTableModel(getEntityClass()).sqlOfSelectAll(sqlConsumer), statementConsumer, getEntityClass());
    }

    /**
     * 查询实体列表（查询所有字段）
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> selectWithPage(int pageIndex, int pageSize) throws NullPointerException, SQLite3Exception {
        return selectWithPage(pageIndex, pageSize, builder -> {}, statement -> {});
    }

    /**
     * 查询实体列表（查询所有字段）
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @param sqlConsumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param statementConsumer sql语句预编译处理
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> selectWithPage(int pageIndex, int pageSize, Consumer<StringBuilder> sqlConsumer, Consumer<SQLite3PreparedStatement> statementConsumer) throws NullPointerException, SQLite3Exception {
        return getJdbcTemplate().queryForList(SQLite3Utils.getClassTableModel(getEntityClass()).sqlOfSelectAllWithPage(sqlConsumer, pageIndex, pageSize), statementConsumer, getEntityClass());
    }

    /**
     * 查询实体列表（查询指定字段）
     * @param fieldNames 待查询字段名称列表
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> selectFields(List<String> fieldNames) throws NullPointerException, SQLite3Exception {
        return selectFields(fieldNames, builder -> {}, statement -> {});
    }

    /**
     * 查询实体列表（查询指定字段）
     * @param fieldNames 待查询字段名称列表
     * @param sqlConsumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param statementConsumer sql语句预编译处理
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> selectFields(List<String> fieldNames, Consumer<StringBuilder> sqlConsumer, Consumer<SQLite3PreparedStatement> statementConsumer) throws NullPointerException, SQLite3Exception {
        return getJdbcTemplate().queryForList(SQLite3Utils.getClassTableModel(getEntityClass()).sqlOfSelectFields(fieldNames, sqlConsumer), statementConsumer, getEntityClass());
    }

    /**
     * 查询实体列表（查询指定字段）
     * @param fieldNames 待查询字段名称列表
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> selectFieldsWithPage(List<String> fieldNames, int pageIndex, int pageSize) throws NullPointerException, SQLite3Exception {
        return selectFieldsWithPage(fieldNames, pageIndex, pageSize, builder -> {}, statement -> {});
    }

    /**
     * 查询实体列表（查询指定字段）
     * @param fieldNames 待查询字段名称列表
     * @param pageIndex 当前页数（从1开始）
     * @param pageSize 分页大小
     * @param sqlConsumer 拼接查询条件: 拼接: where field1=? and field2=?
     * @param statementConsumer sql语句预编译处理
     * @return 实体列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> selectFieldsWithPage(List<String> fieldNames, int pageIndex, int pageSize, Consumer<StringBuilder> sqlConsumer, Consumer<SQLite3PreparedStatement> statementConsumer) throws NullPointerException, SQLite3Exception {
        return getJdbcTemplate().queryForList(SQLite3Utils.getClassTableModel(getEntityClass()).sqlOfSelectFieldsWithPage(fieldNames, sqlConsumer, pageIndex, pageSize), statementConsumer, getEntityClass());
    }

    /**
     * 获取查询SQL: 查询单个实体对象
     * @return 查询SQL
     */
    protected String sqlOfSelectOne() {
        return SQLite3Utils.getClassTableModel(getEntityClass()).sqlOfSelectById();
    }

    /**
     * 根据ID从数据库查询实体对象
     * @param id 实体ID
     * @return 实体对象
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public Entity findWithNoCache(ID id) throws NullPointerException, SQLite3Exception {
        if (id == null) {
            throw new NullPointerException();
        }
        return getJdbcTemplate().queryForObj(sqlOfSelectOne(), statement -> {
            castIDForStatement(statement, 1, id);
        }, getEntityClass());
    }

    /**
     * 根据ID从缓存或数据库查询实体对象
     * @param id 实体ID
     * @return 实体对象
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public Entity find(ID id) throws NullPointerException, SQLite3Exception {
        if (id == null) {
            throw new NullPointerException();
        }
        if (!cacheEnabled) {
            return findWithNoCache(id);
        }
        AtomicReference<Entity> ref = new AtomicReference<>();
        entityCacheReadLock(() -> {
            ref.set(getEntityCacheMap().get(id));
        });
        if (ref.get() == null) {
            entityCacheWriteLock(() -> {
                ref.set(getEntityCacheMap().get(id));
                if (ref.get() == null) {
                    Entity tmpEntity = findWithNoCache(id);
                    if (tmpEntity == null) {
                        tmpEntity = defaultEntity;
                    }
                    getEntityCacheMap().put(id, tmpEntity);
                    ref.set(tmpEntity);
                }
            });
        }
        Entity entity = ref.get();
        if (entity == defaultEntity) {
            entity = null;
        }
        return entity;
    }

    /**
     * 向数据库插入实体模型
     * @param entity 实体模型
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public Entity insertWithNoCache(Entity entity) throws NullPointerException, SQLite3Exception {
        if (entity == null) {
            throw new NullPointerException();
        }
        getJdbcTemplate().insert(entity);
        return entity;
    }

    /**
     * 向数据库插入实体模型（同时更新缓存）
     * @param entity 实体模型
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public Entity insert(Entity entity) throws NullPointerException, SQLite3Exception {
        if (!cacheEnabled) {
            return insertWithNoCache(entity);
        }
        entityCacheWriteLock(() -> {
            insertWithNoCache(entity);
            getEntityCacheMap().put(getIdFieldValue(entity), entity);
        });
        return entity;
    }

    /**
     * 向数据库插入实体模型列表
     * @param entities 实体模型列表
     * @return 实体模型列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> insertWithNoCache(List<Entity> entities) throws NullPointerException, SQLite3Exception {
        if (entities == null) {
            throw new NullPointerException();
        }
        entities.forEach(entity -> {
            if (entity == null) {
                throw new NullPointerException();
            }
        });
        getJdbcTemplate().insert(entities);
        return entities;
    }

    /**
     * 向数据库插入实体模型列表（同时更新缓存）
     * @param entities 实体模型列表
     * @return 实体模型列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public List<Entity> insert(List<Entity> entities) throws NullPointerException, SQLite3Exception {
        if (!cacheEnabled) {
            return insertWithNoCache(entities);
        }
        entityCacheWriteLock(() -> {
            Map<ID, Entity> map = new HashMap<>();
            entities.forEach(entity -> {
                map.put(getIdFieldValue(entity), entity);
            });
            insertWithNoCache(entities);
            getEntityCacheMap().putAll(map);
        });
        return entities;
    }

    /**
     * 向数据库更新实体模型
     * @param entity 实体模型
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public Entity updateWithNoCache(Entity entity) throws NullPointerException, SQLite3Exception {
        if (entity == null) {
            throw new NullPointerException();
        }
        getJdbcTemplate().update(entity);
        return entity;
    }

    /**
     * 向数据库更新实体模型（同时更新缓存）
     * @param entity 实体模型
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public Entity update(Entity entity) throws NullPointerException, SQLite3Exception {
        if (!cacheEnabled) {
            return updateWithNoCache(entity);
        }
        entityCacheWriteLock(() -> {
            ID id = getIdFieldValue(entity);
            updateWithNoCache(entity);
            getEntityCacheMap().put(id, entity);
        });
        return entity;
    }

    /**
     * 向数据库更新实体模型列表
     * @param entities 实体模型列表
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public List<Entity> updateWithNoCache(List<Entity> entities) throws NullPointerException, SQLite3Exception {
        if (entities == null) {
            throw new NullPointerException();
        }
        entities.forEach(entity -> {
            if (entity == null) {
                throw new NullPointerException();
            }
        });
        getJdbcTemplate().update(entities);
        return entities;
    }

    /**
     * 向数据库更新实体模型列表（同时更新缓存）
     * @param entities 实体模型列表
     * @return 实体模型列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public List<Entity> update(List<Entity> entities) throws NullPointerException, SQLite3Exception {
        if (!cacheEnabled) {
            return updateWithNoCache(entities);
        }
        entityCacheWriteLock(() -> {
            Map<ID, Entity> map = new HashMap<>();
            entities.forEach(entity -> {
                map.put(getIdFieldValue(entity), entity);
            });
            updateWithNoCache(entities);
            getEntityCacheMap().putAll(map);
        });
        return entities;
    }

    /**
     * 从数据库删除实体模型
     * @param entity 实体模型
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int deleteWithNoCache(Entity entity) throws NullPointerException, SQLite3Exception {
        return deleteWithNoCache(Collections.singletonList(entity));
    }

    /**
     * 从数据库删除实体模型（同时更新缓存）
     * @param entity 实体模型
     * @return 实体模型
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int delete(Entity entity) throws NullPointerException, SQLite3Exception {
        return delete(Collections.singletonList(entity));
    }

    /**
     * 从数据库删除实体模型列表
     * @param entities 实体模型列表
     * @return 实体模型列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int deleteWithNoCache(List<Entity> entities) throws NullPointerException, SQLite3Exception {
        if (entities == null) {
            throw new NullPointerException();
        }
        List<ID> idList = new ArrayList<>(entities.size());
        entities.forEach(entity -> {
            if (entity == null) {
                throw new NullPointerException();
            }
            idList.add(getIdFieldValue(entity));
        });
        return deleteByIdWithNoCache(idList);
    }

    /**
     * 从数据库删除实体模型列表（同时更新缓存）
     * @param entities 实体模型列表
     * @return 实体模型列表
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int delete(List<Entity> entities) throws NullPointerException, SQLite3Exception {
        if (entities == null) {
            throw new NullPointerException();
        }
        List<ID> idList = new ArrayList<>(entities.size());
        entities.forEach(entity -> {
            if (entity == null) {
                throw new NullPointerException();
            }
            idList.add(getIdFieldValue(entity));
        });
        return deleteById(idList);
    }

    /**
     * 根据ID从数据库删除实体模型
     * @param id 实体模型ID
     * @return 删除成功条数
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int deleteByIdWithNoCache(ID id) throws NullPointerException, SQLite3Exception {
        return deleteByIdWithNoCache(Collections.singletonList(id));
    }

    /**
     * 根据ID从数据库删除实体模型（同时更新缓存）
     * @param id 实体模型ID
     * @return 删除成功条数
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int deleteById(ID id) throws NullPointerException, SQLite3Exception {
        return deleteById(Collections.singletonList(id));
    }

    /**
     * 根据ID列表从数据库删除实体模型
     * @param idList 实体模型ID列表
     * @return 删除成功条数
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    public int deleteByIdWithNoCache(List<ID> idList) throws NullPointerException, SQLite3Exception {
        if (idList == null) {
            throw new NullPointerException();
        }
        idList.forEach(entity -> {
            if (entity == null) {
                throw new NullPointerException();
            }
        });
        TableModel tableModel = SQLite3Utils.getClassTableModel(getEntityClass());
        return jdbcTemplate.batchUpdate(tableModel.sqlOfDeleteById(), idList.size(), (index, statement) -> {
            castIDForStatement(statement, 1, idList.get(index));
        });
    }

    /**
     * 根据ID列表从数据库删除实体模型（同时更新缓存）
     * @param idList 实体模型ID列表
     * @return 删除成功条数
     * @throws NullPointerException NullPointerException
     * @throws SQLite3Exception SQLite3Exception
     */
    @Deprecated
    public int deleteById(List<ID> idList) throws NullPointerException, SQLite3Exception {
        if (!cacheEnabled) {
            return deleteByIdWithNoCache(idList);
        }
        AtomicReference<Integer> ref = new AtomicReference<>();
        entityCacheWriteLock(() -> {
            ref.set(deleteByIdWithNoCache(idList));
            idList.forEach(getEntityCacheMap()::remove);
        });
        return ref.get();
    }

    /**
     * SQL预处理时对数据模型ID强制转型
     * @param statement SQLite3PreparedStatement
     * @param parameterIndex 参数序号
     * @param id 数据模型ID字段值
     */
    protected void castIDForStatement(SQLite3PreparedStatement statement, int parameterIndex, ID id) {
        Class<?> klass = id.getClass();
        if (klass == String.class) {
            statement.setString(parameterIndex, (String) id);
        } else if (klass == int.class || klass == Integer.class) {
            statement.setInt(parameterIndex, (Integer) id);
        } else if (klass == long.class || klass == Long.class) {
            statement.setLong(parameterIndex, (Long) id);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * 获取实体ID字段值
     * @param entity 实体对象
     * @return 实体ID字段值
     */
    @SuppressWarnings("unchecked")
    public ID getIdFieldValue(Entity entity) {
        if (entity == null) {
            throw new NullPointerException();
        }
        return (ID) SQLite3Utils.getClassTableModel(getEntityClass()).getIdFieldValue(entity);
    }

}
