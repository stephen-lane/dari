package com.psddev.dari.db.mysql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Singleton;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateValueUtils;
import com.psddev.dari.db.sql.AbstractSqlDatabase;
import com.psddev.dari.db.sql.SqlVendor;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.UuidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MySQLDatabase extends AbstractSqlDatabase {

    public static final String ENABLE_REPLICATION_CACHE_SUB_SETTING = "enableReplicationCache";
    public static final String REPLICATION_CACHE_SIZE_SUB_SETTING = "replicationCacheSize";

    public static final String DISABLE_REPLICATION_CACHE_QUERY_OPTION = "sql.disableReplicationCache";
    public static final String MYSQL_INDEX_HINT_QUERY_OPTION = "sql.mysqlIndexHint";

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDatabase.class);

    private static final String SHORT_NAME = "MySQL";
    private static final String REPLICATION_CACHE_GET_PROFILER_EVENT = SHORT_NAME + " Replication Cache Get";
    private static final String REPLICATION_CACHE_PUT_PROFILER_EVENT = SHORT_NAME + " Replication Cache Put";

    private static final long DEFAULT_REPLICATION_CACHE_SIZE = 10000L;

    private volatile boolean enableReplicationCache;
    private volatile long replicationCacheMaximumSize;

    private transient volatile Cache<UUID, Object[]> replicationCache;
    private transient volatile MySQLBinaryLogReader mysqlBinaryLogReader;

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
    public void close() {
        try {
            super.close();

        } finally {
            if (mysqlBinaryLogReader != null) {
                LOGGER.info("Stopping MySQL binary log reader");
                mysqlBinaryLogReader.stop();
                mysqlBinaryLogReader = null;
            }
        }
    }

    /**
     * Invalidates all entries in the replication cache.
     */
    public void invalidateReplicationCache() {
        replicationCache.invalidateAll();
    }

    // Creates a previously saved object from the replication cache.
    public <T> T createSavedObjectFromReplicationCache(byte[] typeId, UUID id, byte[] data, Map<String, Object> dataJson, Query<T> query) {
        T object = createSavedObject(typeId, id, query);
        State objectState = State.getInstance(object);

        objectState.setValues(cloneDataJson(dataJson));

        Boolean returnOriginal = query != null ? ObjectUtils.to(Boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION)) : null;

        if (returnOriginal == null) {
            returnOriginal = Boolean.FALSE;
        }

        if (returnOriginal) {
            objectState.getExtras().put(ORIGINAL_DATA_EXTRA, data);
        }

        return swapObjectType(query, object);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cloneDataJson(Map<String, Object> dataJson) {
        return (Map<String, Object>) cloneDataJsonRecursively(dataJson);
    }

    private static Object cloneDataJsonRecursively(Object object) {
        if (object instanceof Map) {
            Map<?, ?> objectMap = (Map<?, ?>) object;
            int objectMapSize = objectMap.size();
            Map<String, Object> clone = objectMapSize <= 8
                    ? new CompactMap<String, Object>()
                    : new LinkedHashMap<String, Object>(objectMapSize);

            for (Map.Entry<?, ?> entry : objectMap.entrySet()) {
                clone.put((String) entry.getKey(), cloneDataJsonRecursively(entry.getValue()));
            }

            return clone;

        } else if (object instanceof List) {
            List<?> objectList = (List<?>) object;
            List<Object> clone = new ArrayList<Object>(objectList.size());

            for (Object item : objectList) {
                clone.add(cloneDataJsonRecursively(item));
            }

            return clone;

        } else {
            return object;
        }
    }

    // Tries to find objects by the given ids from the replication cache.
    // If not found, execute the given query to populate it.
    private <T> List<T> findObjectsFromReplicationCache(List<Object> ids, Query<T> query) {
        List<T> objects = null;

        if (ids == null || ids.isEmpty()) {
            return objects;
        }

        List<UUID> missingIds = null;

        Profiler.Static.startThreadEvent(REPLICATION_CACHE_GET_PROFILER_EVENT);

        String queryGroup = query != null ? query.getGroup() : null;
        Class queryObjectClass = query != null ? query.getObjectClass() : null;

        try {
            for (Object idObject : ids) {
                UUID id = ObjectUtils.to(UUID.class, idObject);

                if (id == null) {
                    continue;
                }

                Object[] value = replicationCache.getIfPresent(id);

                if (value == null) {
                    if (missingIds == null) {
                        missingIds = new ArrayList<UUID>();
                    }

                    missingIds.add(id);
                    continue;
                }

                UUID typeId = ObjectUtils.to(UUID.class, (byte[]) value[0]);

                ObjectType type = typeId != null ? ObjectType.getInstance(typeId) : null;

                // Restrict objects based on the class provided to the Query
                if (type != null && queryObjectClass != null && !query.getObjectClass().isAssignableFrom(type.getObjectClass())) {
                    continue;
                }

                // Restrict objects based on the group provided to the Query
                if (type != null && queryGroup != null && !type.getGroups().contains(queryGroup)) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                T object = createSavedObjectFromReplicationCache((byte[]) value[0], id, (byte[]) value[1], (Map<String, Object>) value[2], query);

                if (object != null) {
                    if (objects == null) {
                        objects = new ArrayList<T>();
                    }

                    objects.add(object);
                }
            }

        } finally {
            Profiler.Static.stopThreadEvent((objects != null ? objects.size() : 0) + " Objects");
        }

        if (missingIds != null && !missingIds.isEmpty()) {
            Profiler.Static.startThreadEvent(REPLICATION_CACHE_PUT_PROFILER_EVENT);

            try {
                SqlVendor vendor = getVendor();
                StringBuilder sqlQuery = new StringBuilder();

                sqlQuery.append("SELECT ");
                vendor.appendIdentifier(sqlQuery, TYPE_ID_COLUMN);
                sqlQuery.append(", ");
                vendor.appendIdentifier(sqlQuery, DATA_COLUMN);
                sqlQuery.append(", ");
                vendor.appendIdentifier(sqlQuery, ID_COLUMN);
                sqlQuery.append(" FROM ");
                vendor.appendIdentifier(sqlQuery, RECORD_TABLE);
                sqlQuery.append(" WHERE ");
                vendor.appendIdentifier(sqlQuery, ID_COLUMN);
                sqlQuery.append(" IN (");

                for (UUID missingId : missingIds) {
                    vendor.appendUuid(sqlQuery, missingId);
                    sqlQuery.append(", ");
                }

                sqlQuery.setLength(sqlQuery.length() - 2);
                sqlQuery.append(")");

                Connection connection = null;
                ConnectionRef extraConnectionRef = new ConnectionRef();
                Statement statement = null;
                ResultSet result = null;

                try {
                    connection = extraConnectionRef.getOrOpen(query);
                    statement = connection.createStatement();
                    result = executeQueryBeforeTimeout(statement, sqlQuery.toString(), 0);

                    while (result.next()) {
                        UUID id = ObjectUtils.to(UUID.class, result.getBytes(3));
                        byte[] data = result.getBytes(2);
                        Map<String, Object> dataJson = unserializeData(data);
                        byte[] typeIdBytes = UuidUtils.toBytes(ObjectUtils.to(UUID.class, dataJson.get(StateValueUtils.TYPE_KEY)));

                        if (!Arrays.equals(typeIdBytes, UuidUtils.ZERO_BYTES) && id != null) {
                            replicationCache.put(id, new Object[] { typeIdBytes, data, dataJson });
                        }

                        UUID typeId = ObjectUtils.to(UUID.class, typeIdBytes);

                        ObjectType type = typeId != null ? ObjectType.getInstance(typeId) : null;

                        // Restrict objects based on the class provided to the Query
                        if (type != null && queryObjectClass != null && !query.getObjectClass().isAssignableFrom(type.getObjectClass())) {
                            continue;
                        }

                        // Restrict objects based on the group provided to the Query
                        if (type != null && queryGroup != null && !type.getGroups().contains(queryGroup)) {
                            continue;
                        }

                        T object = createSavedObjectFromReplicationCache(typeIdBytes, id, data, dataJson, query);

                        if (object != null) {
                            if (objects == null) {
                                objects = new ArrayList<T>();
                            }

                            objects.add(object);
                        }
                    }

                } catch (SQLException error) {
                    throw createQueryException(error, sqlQuery.toString(), query);

                } finally {
                    closeResources(query, connection, statement, result);
                    extraConnectionRef.close();
                }

            } finally {
                Profiler.Static.stopThreadEvent(missingIds.size() + " Objects");
            }
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
                mysqlBinaryLogReader = new MySQLBinaryLogReader(this, replicationCache, ObjectUtils.firstNonNull(getReadDataSource(), getDataSource()));
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
                && !Boolean.TRUE.equals(query.getOptions().get(DISABLE_REPLICATION_CACHE_QUERY_OPTION))
                && mysqlBinaryLogReader != null
                && mysqlBinaryLogReader.isConnected();
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        if (checkReplicationCache(query)) {
            List<Object> ids = query.findIdOnlyQueryValues();

            if (ids != null && !ids.isEmpty()) {
                List<T> objects = findObjectsFromReplicationCache(ids, query);

                return objects != null ? objects : new ArrayList<T>();
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
}
