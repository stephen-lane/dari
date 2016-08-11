package com.psddev.dari.db.mysql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Singleton;
import com.psddev.dari.db.SqlVendor;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateSerializer;
import com.psddev.dari.db.StateValueUtils;
import com.psddev.dari.db.sql.AbstractSqlDatabase;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.UuidUtils;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.util.mysql.MySQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MySQLDatabase extends AbstractSqlDatabase implements AutoCloseable {

    private static final DataType<UUID> UUID_TYPE = MySQLDataType.BINARY.asConvertedDataType(new Converter<byte[], UUID>() {

        @Override
        public UUID from(byte[] bytes) {
            return bytes != null ? UuidUtils.fromBytes(bytes) : null;
        }

        @Override
        public byte[] to(UUID uuid) {
            return uuid != null ? UuidUtils.toBytes(uuid) : null;
        }

        @Override
        public Class<byte[]> fromType() {
            return byte[].class;
        }

        @Override
        public Class<UUID> toType() {
            return UUID.class;
        }
    });

    public static final String ENABLE_REPLICATION_CACHE_SUB_SETTING = "enableReplicationCache";
    public static final String REPLICATION_CACHE_SIZE_SUB_SETTING = "replicationCacheSize";

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDatabase.class);

    private static final String SHORT_NAME = "MySQL";
    private static final String REPLICATION_CACHE_GET_PROFILER_EVENT = SHORT_NAME + " Replication Cache Get";
    private static final String REPLICATION_CACHE_PUT_PROFILER_EVENT = SHORT_NAME + " Replication Cache Put";

    private static final long DEFAULT_REPLICATION_CACHE_SIZE = 10000L;

    private volatile Boolean binlogFormatStatement;
    private volatile boolean enableReplicationCache;
    private volatile long replicationCacheMaximumSize;

    private transient volatile Cache<UUID, Object[]> replicationCache;
    private transient volatile MySQLBinaryLogReader mysqlBinaryLogReader;
    private final transient ConcurrentMap<Class<?>, UUID> singletonIds = new ConcurrentHashMap<>();

    @Override
    protected DataType<UUID> uuidType() {
        return UUID_TYPE;
    }

    @Override
    protected Condition stContains(Field<Object> x, Field<Object> y) {
        return DSL.condition("MBRContains({0}, {1})", x, y);
    }

    @Override
    protected Field<Object> stGeomFromText(Field<String> wkt) {
        return DSL.field("GeomFromText({0})", wkt);
    }

    @Override
    protected Field<Double> stLength(Field<Object> field) {
        return DSL.field("GLength({0})", Double.class, field);
    }

    @Override
    protected Field<Object> stMakeLine(Field<Object> x, Field<Object> y) {
        return DSL.field("LineString({0}, {1})", x, y);
    }

    @Override
    protected void setTransactionIsolation(Connection connection) throws SQLException {
        if (binlogFormatStatement == null) {
            synchronized (this) {
                if (binlogFormatStatement == null) {
                    try (Statement statement = connection.createStatement();
                            ResultSet result = statement.executeQuery("SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format')")) {

                        boolean logBin = false;

                        while (result.next()) {
                            String name = result.getString(1);
                            String value = result.getString(2);

                            if ("binlog_format".equalsIgnoreCase(name)) {
                                binlogFormatStatement = "STATEMENT".equalsIgnoreCase(value);

                            } else if ("log_bin".equalsIgnoreCase(name)) {
                                logBin = !"OFF".equalsIgnoreCase(value);
                            }
                        }

                        binlogFormatStatement = logBin && Boolean.TRUE.equals(binlogFormatStatement);

                        if (binlogFormatStatement) {
                            LOGGER.warn("Can't set transaction isolation to"
                                    + " READ COMMITTED because binlog_format"
                                    + " is set to STATEMENT. Please set it to"
                                    + " MIXED (my.cnf: binlog_format = mixed)"
                                    + " to prevent reduced performance under"
                                    + " load.");
                        }
                    }
                }
            }
        }

        if (!binlogFormatStatement) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    public boolean isEnableReplicationCache() {
        return enableReplicationCache;
    }

    public void setEnableReplicationCache(boolean enableReplicationCache) {
        this.enableReplicationCache = enableReplicationCache;
    }

    public void setReplicationCacheMaximumSize(long replicationCacheMaximumSize) {
        this.replicationCacheMaximumSize = replicationCacheMaximumSize;
    }

    public long getReplicationCacheMaximumSize() {
        return this.replicationCacheMaximumSize;
    }

    @Override
    protected SQLDialect dialect() {
        return SQLDialect.MYSQL;
    }

    @Override
    public SqlVendor getMetricVendor() {
        return new SqlVendor.MySQL();
    }

    @Override
    public void close() {
        if (mysqlBinaryLogReader != null) {
            LOGGER.info("Stopping MySQL binary log reader");
            mysqlBinaryLogReader.stop();
            mysqlBinaryLogReader = null;
        }
    }

    /**
     * Invalidates all entries in the replication cache.
     */
    public void invalidateReplicationCache() {
        replicationCache.invalidateAll();
    }

    @Override
    protected <T> T createSavedObjectUsingResultSet(ResultSet resultSet, Query<T> query) throws SQLException {
        T object = super.createSavedObjectUsingResultSet(resultSet, query);

        if (object instanceof Singleton) {
            singletonIds.put(object.getClass(), State.getInstance(object).getId());
        }

        return object;
    }

    // Creates a previously saved object from the replication cache.
    @SuppressWarnings("unchecked")
    protected <T> T createSavedObjectFromReplicationCache(UUID id, byte[] data, Map<String, Object> dataJson, Query<T> query) {
        UUID typeId = ObjectUtils.to(UUID.class, dataJson.get(StateValueUtils.TYPE_KEY));
        T object = createSavedObject(typeId, id, query);
        State state = State.getInstance(object);

        state.setValues((Map<String, Object>) cloneJson(dataJson));

        if (query != null && ObjectUtils.to(boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION))) {
            state.getExtras().put(ORIGINAL_DATA_EXTRA, data);
        }

        return swapObjectType(query, object);
    }

    @SuppressWarnings("unchecked")
    private static Object cloneJson(Object object) {
        if (object instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) object;
            Map<String, Object> clone = new CompactMap<>(map.size());

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                clone.put((String) entry.getKey(), cloneJson(entry.getValue()));
            }

            return clone;

        } else if (object instanceof List) {
            return ((List<Object>) object).stream()
                    .map(MySQLDatabase::cloneJson)
                    .collect(Collectors.toList());

        } else {
            return object;
        }
    }

    // Tries to find objects by the given ids from the replication cache.
    // If not found, execute the given query to populate it.
    private <T> List<T> findObjectsFromReplicationCache(List<Object> ids, Query<T> query) {
        List<T> objects = null;
        List<UUID> missingIds = null;

        Profiler.Static.startThreadEvent(REPLICATION_CACHE_GET_PROFILER_EVENT);

        try {
            for (Object idObject : ids) {
                UUID id = ObjectUtils.to(UUID.class, idObject);

                if (id == null) {
                    continue;
                }

                Object[] value = replicationCache.getIfPresent(id);

                if (value == null) {
                    if (missingIds == null) {
                        missingIds = new ArrayList<>();
                    }

                    missingIds.add(id);
                    continue;
                }

                UUID typeId = ObjectUtils.to(UUID.class, value[0]);
                byte[] data = (byte[]) value[1];
                @SuppressWarnings("unchecked")
                Map<String, Object> dataJson = (Map<String, Object>) value[2];

                objects = addObjects(
                        objects,
                        typeId,
                        id,
                        data,
                        dataJson,
                        query);
            }

        } finally {
            Profiler.Static.stopThreadEvent((objects != null ? objects.size() : 0) + " Objects");
        }

        if (missingIds != null && !missingIds.isEmpty()) {
            Profiler.Static.startThreadEvent(REPLICATION_CACHE_PUT_PROFILER_EVENT);

            try {
                String sqlQuery = DSL.using(dialect())
                        .select(recordIdField, recordDataField)
                        .from(recordTable)
                        .where(recordIdField.in(missingIds))
                        .getSQL(ParamType.INLINED);

                List<T> selectObjects = objects;

                objects = select(sqlQuery, query, result -> {
                    List<T> resultObjects = selectObjects;

                    while (result.next()) {
                        UUID id = ObjectUtils.to(UUID.class, result.getBytes(1));

                        if (id == null) {
                            continue;
                        }

                        byte[] data = result.getBytes(2);
                        Map<String, Object> dataJson = StateSerializer.deserialize(data);
                        UUID typeId = ObjectUtils.to(UUID.class, dataJson.get(StateValueUtils.TYPE_KEY));

                        if (!UuidUtils.ZERO_UUID.equals(typeId)) {
                            replicationCache.put(id, new Object[] { UuidUtils.toBytes(typeId), data, dataJson });
                        }

                        resultObjects = addObjects(
                                resultObjects,
                                typeId,
                                id,
                                data,
                                dataJson,
                                query);
                    }

                    return resultObjects;
                });

            } finally {
                Profiler.Static.stopThreadEvent(missingIds.size() + " Objects");
            }
        }

        return objects;
    }

    private <T> List<T> addObjects(
            List<T> objects,
            UUID typeId,
            UUID id,
            byte[] data,
            Map<String, Object> dataJson,
            Query<T> query) {

        if (typeId != null && query != null) {
            ObjectType type = ObjectType.getInstance(typeId);

            if (type != null) {
                Class<?> queryObjectClass = query.getObjectClass();

                if (queryObjectClass != null && !query.getObjectClass().isAssignableFrom(type.getObjectClass())) {
                    return objects;
                }

                String queryGroup = query.getGroup();

                if (queryGroup != null && !type.getGroups().contains(queryGroup)) {
                    return objects;
                }
            }
        }

        T object = createSavedObjectFromReplicationCache(id, data, dataJson, query);

        if (object != null) {
            if (objects == null) {
                objects = new ArrayList<>();
            }

            objects.add(object);
        }

        return objects;
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        super.doInitialize(settingsKey, settings);

        setEnableReplicationCache(ObjectUtils.to(boolean.class, settings.get(ENABLE_REPLICATION_CACHE_SUB_SETTING)));
        Long replicationCacheMaxSize = ObjectUtils.to(Long.class, settings.get(REPLICATION_CACHE_SIZE_SUB_SETTING));
        setReplicationCacheMaximumSize(replicationCacheMaxSize != null ? replicationCacheMaxSize : DEFAULT_REPLICATION_CACHE_SIZE);

        if (isEnableReplicationCache()
                && (mysqlBinaryLogReader == null
                || !mysqlBinaryLogReader.isRunning())) {

            replicationCache = CacheBuilder.newBuilder().maximumSize(getReplicationCacheMaximumSize()).build();

            try {
                LOGGER.info("Starting MySQL binary log reader");
                mysqlBinaryLogReader = new MySQLBinaryLogReader(this, replicationCache, getReadDataSource(), recordTable.getName());
                mysqlBinaryLogReader.start();

            } catch (IllegalArgumentException error) {
                setEnableReplicationCache(false);
                LOGGER.warn("Can't start MySQL binary log reader!", error);
            }
        }
    }

    private boolean checkReplicationCache(Query<?> query) {
        return query.isCache()
                && isEnableReplicationCache()
                && mysqlBinaryLogReader != null
                && mysqlBinaryLogReader.isConnected();
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        if (checkReplicationCache(query)) {
            List<Object> ids = query.findIdOnlyQueryValues();

            if (ids != null && !ids.isEmpty()) {
                List<T> objects = findObjectsFromReplicationCache(ids, query);

                return objects != null ? objects : new ArrayList<>();
            }
        }

        return super.readAll(query);
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        if (query.getSorters().isEmpty()) {

            Predicate predicate = query.getPredicate();
            if (predicate instanceof CompoundPredicate) {

                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
                if (PredicateParser.OR_OPERATOR.equals(compoundPredicate.getOperator())) {

                    for (Predicate child : compoundPredicate.getChildren()) {
                        Query<T> childQuery = query.clone();
                        childQuery.setPredicate(child);

                        T first = readFirst(childQuery);
                        if (first != null) {
                            return first;
                        }
                    }

                    return null;
                }
            }
        }

        if (checkReplicationCache(query)) {
            Class<?> objectClass = query.getObjectClass();
            List<Object> ids;

            if (objectClass != null
                    && Singleton.class.isAssignableFrom(objectClass)
                    && query.getPredicate() == null) {

                UUID id = singletonIds.get(objectClass);
                ids = id != null ? Collections.singletonList(id) : null;

            } else {
                ids = query.findIdOnlyQueryValues();
            }

            if (ids != null && !ids.isEmpty()) {
                List<T> objects = findObjectsFromReplicationCache(ids, query);

                return objects == null || objects.isEmpty() ? null : objects.get(0);
            }
        }

        return super.readFirst(query);
    }

    @Override
    protected boolean shouldUseSavepoint() {
        return false;
    }
}
