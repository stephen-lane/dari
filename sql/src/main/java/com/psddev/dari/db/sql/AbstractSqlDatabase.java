package com.psddev.dari.db.sql;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.psddev.dari.db.AbstractDatabase;
import com.psddev.dari.db.AbstractGrouping;
import com.psddev.dari.db.AtomicOperation;
import com.psddev.dari.db.DatabaseException;
import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.MetricSqlDatabase;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Singleton;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateValueUtils;
import com.psddev.dari.db.UpdateNotifier;
import com.zaxxer.hikari.HikariDataSource;
import org.iq80.snappy.Snappy;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.SettingsException;
import com.psddev.dari.util.Stats;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;

/** Database backed by a SQL engine. */
public abstract class AbstractSqlDatabase extends AbstractDatabase<Connection> implements MetricSqlDatabase {

    public static final String DATA_SOURCE_SETTING = "dataSource";
    public static final String DATA_SOURCE_JNDI_NAME_SETTING = "dataSourceJndiName";
    public static final String JDBC_DRIVER_CLASS_SETTING = "jdbcDriverClass";
    public static final String JDBC_URL_SETTING = "jdbcUrl";
    public static final String JDBC_USER_SETTING = "jdbcUser";
    public static final String JDBC_PASSWORD_SETTING = "jdbcPassword";
    public static final String JDBC_POOL_SIZE_SETTING = "jdbcPoolSize";

    public static final String READ_DATA_SOURCE_SETTING = "readDataSource";
    public static final String READ_DATA_SOURCE_JNDI_NAME_SETTING = "readDataSourceJndiName";
    public static final String READ_JDBC_DRIVER_CLASS_SETTING = "readJdbcDriverClass";
    public static final String READ_JDBC_URL_SETTING = "readJdbcUrl";
    public static final String READ_JDBC_USER_SETTING = "readJdbcUser";
    public static final String READ_JDBC_PASSWORD_SETTING = "readJdbcPassword";
    public static final String READ_JDBC_POOL_SIZE_SETTING = "readJdbcPoolSize";

    public static final String CATALOG_SUB_SETTING = "catalog";
    public static final String METRIC_CATALOG_SUB_SETTING = "metricCatalog";
    public static final String VENDOR_CLASS_SETTING = "vendorClass";
    public static final String COMPRESS_DATA_SUB_SETTING = "compressData";

    public static final String INDEX_SPATIAL_SUB_SETTING = "indexSpatial";

    public static final String RECORD_TABLE = "Record";
    public static final String SYMBOL_TABLE = "Symbol";
    public static final String ID_COLUMN = "id";
    public static final String TYPE_ID_COLUMN = "typeId";
    public static final String DATA_COLUMN = "data";
    public static final String SYMBOL_ID_COLUMN = "symbolId";
    public static final String UPDATE_DATE_COLUMN = "updateDate";
    public static final String VALUE_COLUMN = "value";

    public static final String CONNECTION_QUERY_OPTION = "sql.connection";
    public static final String RETURN_ORIGINAL_DATA_QUERY_OPTION = "sql.returnOriginalData";
    public static final String USE_JDBC_FETCH_SIZE_QUERY_OPTION = "sql.useJdbcFetchSize";
    public static final String USE_READ_DATA_SOURCE_QUERY_OPTION = "sql.useReadDataSource";
    public static final String SKIP_INDEX_STATE_EXTRA = "sql.skipIndex";

    public static final String INDEX_TABLE_INDEX_OPTION = "sql.indexTable";

    public static final String ORIGINAL_DATA_EXTRA = "sql.originalData";

    public static final String SUB_DATA_COLUMN_ALIAS_PREFIX = "subData_";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSqlDatabase.class);

    private static final String SHORT_NAME = "SQL";
    private static final Stats STATS = new Stats(SHORT_NAME);
    private static final String CONNECTION_ERROR_STATS_OPERATION = "Connection Error";
    private static final String QUERY_STATS_OPERATION = "Query";
    private static final String UPDATE_STATS_OPERATION = "Update";
    private static final String QUERY_PROFILER_EVENT = SHORT_NAME + " " + QUERY_STATS_OPERATION;
    private static final String UPDATE_PROFILER_EVENT = SHORT_NAME + " " + UPDATE_STATS_OPERATION;
    private static final long NOW_EXPIRATION_SECONDS = 300;
    public static final long DEFAULT_DATA_CACHE_SIZE = 10000L;

    private static final List<AbstractSqlDatabase> INSTANCES = new ArrayList<>();

    {
        INSTANCES.add(this);
    }

    private volatile DataSource dataSource;
    private volatile DataSource readDataSource;
    private volatile String catalog;
    private volatile String metricCatalog;
    private transient volatile String defaultCatalog;
    private volatile SqlVendor vendor;
    private volatile boolean compressData;
    private volatile boolean indexSpatial;

    protected final transient ConcurrentMap<Class<?>, UUID> singletonIds = new ConcurrentHashMap<>();
    private final List<UpdateNotifier<?>> updateNotifiers = new ArrayList<>();

    // Cache that stores the difference between the local and the database
    // time. This is used to calculate a more accurate time without querying
    // the database all the time.
    private final transient Supplier<Long> nowOffset = Suppliers.memoizeWithExpiration(() -> {
        try {
            Connection connection = openConnection();

            try (DSLContext context = openContext(connection)) {
                return System.currentTimeMillis() - context
                        .select(DSL.currentTimestamp())
                        .fetchOptional()
                        .map(r -> r.value1().getTime())
                        .orElse(0L);

            } finally {
                closeConnection(connection);
            }

        } catch (Exception error) {
            return 0L;
        }

    }, 5, TimeUnit.MINUTES);

    // Cache that stores all the table and column names.
    private final transient Lazy<Map<String, Set<String>>> tableColumnNames = new Lazy<Map<String, Set<String>>>() {

        @Override
        protected Map<String, Set<String>> create() {
            Connection connection = openAnyConnection();

            try (DSLContext context = openContext(connection)) {
                return context.meta().getTables().stream()
                        .collect(Collectors.toMap(
                                t -> t.getName().toLowerCase(Locale.ENGLISH),
                                t -> Arrays.stream(t.fields())
                                        .map(c -> c.getName().toLowerCase(Locale.ENGLISH))
                                        .collect(Collectors.toSet())));

            } catch (DataAccessException error) {
                throw convertJooqError(error, null);

            } finally {
                closeConnection(connection);
            }
        }
    };

    // Cache that stores all the symbol IDs.
    private final transient Lazy<Map<String, Integer>> symbolIds = new Lazy<Map<String, Integer>>() {

        @Override
        protected Map<String, Integer> create() {
            Connection connection = openConnection();

            try (DSLContext context = openContext(connection)) {
                SqlSchema schema = schema();
                ResultQuery<Record2<Integer, String>> query = context
                        .select(schema.symbolIdField(), schema.symbolValueField())
                        .from(schema.symbolTable());

                try {
                    Map<String, Integer> symbolIds = new ConcurrentHashMap<>();
                    query.fetch().forEach(r -> symbolIds.put(r.value2(), r.value1()));
                    return symbolIds;

                } catch (DataAccessException error) {
                    throw convertJooqError(error, query);
                }

            } finally {
                closeConnection(connection);
            }
        }
    };

    /**
     * Quotes the given {@code value} so that it's safe to use
     * in a SQL query.
     */
    public static String quoteValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof byte[]) {
            return "X'" + StringUtils.hex((byte[]) value) + "'";
        } else {
            return "'" + value.toString().replace("'", "''").replace("\\", "\\\\") + "'";
        }
    }

    /** Closes all resources used by all instances. */
    public static void closeAll() {
        INSTANCES.forEach(AbstractSqlDatabase::close);
        INSTANCES.clear();
    }

    /**
     * Creates an {@link SqlDatabaseException} that occurred during
     * an execution of a query.
     */
    protected SqlDatabaseException createQueryException(
            SQLException error,
            String sqlQuery,
            Query<?> query) {

        String message = error.getMessage();
        if (error instanceof SQLTimeoutException || message.contains("timeout")) {
            return new SqlDatabaseException.ReadTimeout(this, error, sqlQuery, query);
        } else {
            return new SqlDatabaseException(this, error, sqlQuery, query);
        }
    }

    /** Returns the JDBC data source used for general database operations. */
    public DataSource getDataSource() {
        return dataSource;
    }

    private static final Map<String, Class<? extends SqlVendor>> VENDOR_CLASSES; static {
        Map<String, Class<? extends SqlVendor>> m = new HashMap<>();
        m.put("H2", SqlVendor.H2.class);
        m.put("MySQL", SqlVendor.MySQL.class);
        m.put("PostgreSQL", SqlVendor.PostgreSQL.class);
        m.put("EnterpriseDB", SqlVendor.PostgreSQL.class);
        m.put("Oracle", SqlVendor.Oracle.class);
        VENDOR_CLASSES = m;
    }

    /** Sets the JDBC data source used for general database operations. */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        if (dataSource == null) {
            return;
        }

        synchronized (this) {
            try {
                boolean writable = false;

                if (vendor == null) {
                    Connection connection;

                    try {
                        connection = openConnection();
                        writable = true;

                    } catch (DatabaseException error) {
                        LOGGER.debug("Can't read vendor information from the writable server!", error);
                        connection = openReadConnection();
                    }

                    try {
                        defaultCatalog = connection.getCatalog();
                        DatabaseMetaData meta = connection.getMetaData();
                        String vendorName = meta.getDatabaseProductName();
                        Class<? extends SqlVendor> vendorClass = VENDOR_CLASSES.get(vendorName);

                        LOGGER.info(
                                "Initializing SQL vendor for [{}]: [{}] -> [{}]",
                                new Object[] { getName(), vendorName, vendorClass });

                        vendor = vendorClass != null ? TypeDefinition.getInstance(vendorClass).newInstance() : new SqlVendor();

                    } finally {
                        closeConnection(connection);
                    }
                }

                invalidateCaches();

                if (writable) {
                    vendor.setUp(this);
                    invalidateCaches();
                }

            } catch (IOException error) {
                throw new IllegalStateException(error);

            } catch (SQLException error) {
                throw new SqlDatabaseException(this, "Can't check for required tables!", error);
            }
        }
    }

    /** Returns the JDBC data source used exclusively for read operations. */
    public DataSource getReadDataSource() {
        return this.readDataSource;
    }

    /** Sets the JDBC data source used exclusively for read operations. */
    public void setReadDataSource(DataSource readDataSource) {
        this.readDataSource = readDataSource;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;

        try {
            getVendor().setUp(this);
            invalidateCaches();

        } catch (IOException error) {
            throw new IllegalStateException(error);

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't check for required tables!", error);
        }
    }

    /** Returns the catalog that contains the Metric table.
     *
     * @return May be {@code null}.
     *
     **/
    public String getMetricCatalog() {
        return metricCatalog;
    }

    public void setMetricCatalog(String metricCatalog) {
        if (ObjectUtils.isBlank(metricCatalog)) {
            this.metricCatalog = null;

        } else {
            this.metricCatalog = metricCatalog;
        }
    }

    /** Returns the vendor-specific SQL engine information. */
    public SqlVendor getVendor() {
        return vendor;
    }

    /** Sets the vendor-specific SQL engine information. */
    public void setVendor(SqlVendor vendor) {
        this.vendor = vendor;
    }

    /** Returns {@code true} if the data should be compressed. */
    public boolean isCompressData() {
        return compressData;
    }

    /** Sets whether the data should be compressed. */
    public void setCompressData(boolean compressData) {
        this.compressData = compressData;
    }

    public boolean isIndexSpatial() {
        return indexSpatial;
    }

    public void setIndexSpatial(boolean indexSpatial) {
        this.indexSpatial = indexSpatial;
    }

    /**
     * Opens a connection to the underlying SQL server, preferably the writable
     * one that has the most up-to-date data.
     *
     * @return Never {@code null}.
     */
    protected Connection openAnyConnection() {
        try {
            return openConnection();

        } catch (DatabaseException error) {
            LOGGER.debug("Can't open a writable connection! Trying a readable one instead...", error);
            return openReadConnection();
        }
    }

    /**
     * @return Never {@code null}.
     */
    protected abstract SQLDialect dialect();

    /**
     * @return Never {@code null}.
     */
    protected abstract SqlSchema schema();

    /**
     * @return Never {@code null}.
     */
    protected DSLContext openContext(Connection connection) {
        return DSL.using(connection, dialect());
    }

    private SqlDatabaseException convertJooqError(DataAccessException error, org.jooq.Query query) {
        Throwable cause = error.getCause();
        SQLException sqlError = cause instanceof SQLException ? (SQLException) cause : null;

        if (sqlError != null) {
            String message = sqlError.getMessage();

            if (sqlError instanceof SQLTimeoutException
                    || (message != null
                    && message.contains("timeout"))) {

                return new SqlDatabaseException.ReadTimeout(this, sqlError, query.getSQL(), null);
            }
        }

        return new SqlDatabaseException(this, sqlError, query.getSQL(), null);
    }

    @Override
    public long now() {
        return System.currentTimeMillis() - nowOffset.get();
    }

    /**
     * Invalidates all caches.
     *
     * <p>This forces all metadata to be re-fetched from the database in the
     * following methods:</p>
     *
     * <ul>
     *     <li>{@link #hasTable(String)}</li>
     *     <li>{@link #hasColumn(String, String)}</li>
     *     <li>{@link #getSymbolId(String)}</li>
     *     <li>{@link #getReadSymbolId(String)}</li>
     * </ul>
     */
    public void invalidateCaches() {
        tableColumnNames.reset();
        symbolIds.reset();
    }

    /**
     * Returns {@code true} if a table exists with the given {@code name}.
     *
     * @param name If {@code null}, always returns {@code false}.
     */
    public boolean hasTable(String name) {
        return name != null
                && tableColumnNames.get().containsKey(name.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Returns {@code true} if the table with the given {@code tableName}
     * contains a column with the given {@code columnName}.
     *
     * @param tableName If {@code null}, always returns {@code false}.
     * @param columnName If {@code null}, always returns {@code false}.
     */
    public boolean hasColumn(String tableName, String columnName) {
        if (tableName == null || columnName == null) {
            return false;

        } else {
            Set<String> columnNames = tableColumnNames.get().get(tableName.toLowerCase(Locale.ENGLISH));

            return columnNames != null
                    && columnNames.contains(columnName.toLowerCase(Locale.ENGLISH));
        }
    }

    /**
     * Returns a unique ID for the given {@code symbol}, creating one if
     * necessary.
     *
     * @param symbol Can't be {@code null}.
     */
    public int getSymbolId(String symbol) {
        return findSymbolId(symbol, true);
    }

    /**
     * Returns a unique ID for the given {@code symbol}, or {@code -1} if
     * it's not available.
     *
     * @param symbol Can't be {@code null}.
     */
    public int getReadSymbolId(String symbol) {
        return findSymbolId(symbol, false);
    }

    private int findSymbolId(String symbol, boolean create) {
        Preconditions.checkNotNull(symbol);

        Integer id = symbolIds.get().get(symbol);

        if (id != null) {
            return id;
        }

        Connection connection = openAnyConnection();

        try (DSLContext context = openContext(connection)) {
            SqlSchema schema = schema();

            if (create) {
                org.jooq.Query createQuery = context
                        .insertInto(schema.symbolTable(), schema.symbolValueField())
                        .values(symbol)
                        .onDuplicateKeyIgnore();

                try {
                    createQuery.execute();

                } catch (DataAccessException error) {
                    throw convertJooqError(error, createQuery);
                }
            }

            ResultQuery<Record1<Integer>> selectQuery = context
                    .select(schema.symbolIdField())
                    .from(schema.symbolTable())
                    .where(schema.symbolValueField().eq(symbol));

            try {
                id = selectQuery
                        .fetchOptional()
                        .map(Record1::value1)
                        .orElse(null);

                if (id != null) {
                    symbolIds.get().put(symbol, id);
                    onSymbolIdUpdate(symbol, id);
                    return id;

                } else {
                    return -1;
                }

            } catch (DataAccessException error) {
                throw convertJooqError(error, selectQuery);
            }

        } finally {
            closeConnection(connection);
        }
    }

    /**
     * Called when the given {@code symbol}'s {@code id} is updated.
     *
     * @param symbol Can't be {@code null}.
     * @param id Can't be {@code null}.
     */
    protected void onSymbolIdUpdate(String symbol, int id) {
    }

    /** Closes any resources used by this database. */
    public void close() {
        DataSource dataSource = getDataSource();
        if (dataSource instanceof HikariDataSource) {
            LOGGER.info("Closing connection pool in {}", getName());
            ((HikariDataSource) dataSource).close();
        }

        DataSource readDataSource = getReadDataSource();
        if (readDataSource instanceof HikariDataSource) {
            LOGGER.info("Closing read connection pool in {}", getName());
            ((HikariDataSource) readDataSource).close();
        }

        setDataSource(null);
        setReadDataSource(null);
    }

    private String addComment(String sql, Query<?> query) {
        if (query != null) {
            String comment = query.getComment();

            if (!ObjectUtils.isBlank(comment)) {
                return "/*" + comment + "*/ " + sql;
            }
        }

        return sql;
    }

    /**
     * Builds an SQL statement that can be used to get a count of all
     * objects matching the given {@code query}.
     */
    public String buildCountStatement(Query<?> query) {
        return addComment(new SqlQuery(this, query).countStatement(), query);
    }

    /**
     * Builds an SQL statement that can be used to delete all rows
     * matching the given {@code query}.
     */
    public String buildDeleteStatement(Query<?> query) {
        return addComment(new SqlQuery(this, query).deleteStatement(), query);
    }

    /**
     * Builds an SQL statement that can be used to get all objects
     * grouped by the values of the given {@code groupFields}.
     */
    public String buildGroupStatement(Query<?> query, String... groupFields) {
        return addComment(new SqlQuery(this, query).groupStatement(groupFields), query);
    }

    /**
     * Builds an SQL statement that can be used to get when the objects
     * matching the given {@code query} were last updated.
     */
    public String buildLastUpdateStatement(Query<?> query) {
        return addComment(new SqlQuery(this, query).lastUpdateStatement(), query);
    }

    /**
     * Maintains a cache of Querys to SQL select statements.
     */
    private final LoadingCache<Query<?>, String> sqlQueryCache = CacheBuilder
            .newBuilder()
            .maximumSize(5000)
            .concurrencyLevel(20)
            .build(new CacheLoader<Query<?>, String>() {
                @Override
                @ParametersAreNonnullByDefault
                public String load(Query<?> query) throws Exception {
                    return new SqlQuery(AbstractSqlDatabase.this, query).selectStatement();
                }
            });

    /**
     * Builds an SQL statement that can be used to list all rows
     * matching the given {@code query}.
     */
    public String buildSelectStatement(Query<?> query) {
        try {
            Query<?> strippedQuery = query.clone();
            // Remove any possibility that multiple CachingDatabases will be cached in the sqlQueryCache.
            strippedQuery.setDatabase(this);
            strippedQuery.getOptions().remove(State.REFERENCE_RESOLVING_QUERY_OPTION);
            return addComment(sqlQueryCache.getUnchecked(strippedQuery), query);
        } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new DatabaseException(this, cause);
            }
        }
    }

    // Closes all the given SQL resources safely.
    protected void closeResources(Query<?> query, Connection connection, Statement statement, ResultSet result) {
        if (result != null) {
            try {
                result.close();
            } catch (SQLException error) {
                // Not likely and probably harmless.
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException error) {
                // Not likely and probably harmless.
            }
        }

        Object queryConnection;

        if (connection != null
                && (query == null
                || (queryConnection = query.getOptions().get(CONNECTION_QUERY_OPTION)) == null
                || !connection.equals(queryConnection))) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException ex) {
                // Not likely and probably harmless.
            }
        }
    }

    private byte[] serializeState(State state) {
        Map<String, Object> values = state.getSimpleValues();
        byte[] dataBytes = ObjectUtils.toJson(values).getBytes(StandardCharsets.UTF_8);

        if (isCompressData()) {
            byte[] compressed = new byte[Snappy.maxCompressedLength(dataBytes.length)];
            int compressedLength = Snappy.compress(dataBytes, 0, dataBytes.length, compressed, 0);

            dataBytes = new byte[compressedLength + 1];
            dataBytes[0] = 's';
            System.arraycopy(compressed, 0, dataBytes, 1, compressedLength);
        }

        return dataBytes;
    }

    private static byte[] decodeData(byte[] dataBytes) {
        char format;

        while (true) {
            format = (char) dataBytes[0];

            if (format == 's') {
                dataBytes = Snappy.uncompress(dataBytes, 1, dataBytes.length - 1);

            } else if (format == '{') {
                return dataBytes;

            } else {
                break;
            }
        }

        throw new IllegalStateException(String.format(
                "Unknown format! ([%s])",
                format));
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> unserializeData(byte[] dataBytes) {
        char format;

        while (true) {
            format = (char) dataBytes[0];

            if (format == 's') {
                dataBytes = Snappy.uncompress(dataBytes, 1, dataBytes.length - 1);

            } else if (format == '{') {
                return (Map<String, Object>) ObjectUtils.fromJson(dataBytes);

            } else {
                break;
            }
        }

        throw new IllegalStateException(String.format(
                "Unknown format! ([%s])", format));
    }

    protected class ConnectionRef {

        private Connection connection;

        public ConnectionRef() {
        }

        public Connection getOrOpen(Query<?> query) {
            if (connection == null) {
                connection = AbstractSqlDatabase.super.openQueryConnection(query);
            }
            return connection;
        }

        public void close() {
            if (connection != null) {
                try {
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                } catch (SQLException error) {
                    // Not likely and probably harmless.
                }
            }
        }
    }

    // Creates a previously saved object using the given resultSet.
    private <T> T createSavedObjectWithResultSet(
            ResultSet resultSet,
            Query<T> query,
            ConnectionRef extraConnectionRef)
            throws SQLException {

        T object = createSavedObject(resultSet.getObject(2), resultSet.getObject(1), query);
        State objectState = State.getInstance(object);

        if (object instanceof Singleton) {
            singletonIds.put(object.getClass(), objectState.getId());
        }

        if (!objectState.isReferenceOnly()) {
            byte[] data = resultSet.getBytes(3);

            if (data != null) {
                byte[] decodedData = decodeData(data);
                @SuppressWarnings("unchecked")
                Map<String, Object> unserializedData = (Map<String, Object>) ObjectUtils.fromJson(decodedData);

                objectState.setValues(unserializedData);
                objectState.getExtras().put(DATA_LENGTH_EXTRA, decodedData.length);
                Boolean returnOriginal = ObjectUtils.to(Boolean.class, query.getOptions().get(RETURN_ORIGINAL_DATA_QUERY_OPTION));
                if (returnOriginal == null) {
                    returnOriginal = Boolean.FALSE;
                }
                if (returnOriginal) {
                    objectState.getExtras().put(ORIGINAL_DATA_EXTRA, data);
                }
            }
        }

        ResultSetMetaData meta = resultSet.getMetaData();
        Object subId = null, subTypeId = null;
        byte[] subData;

        for (int i = 4, count = meta.getColumnCount(); i <= count; ++ i) {
            String columnName = meta.getColumnLabel(i);
            if (columnName.startsWith(SUB_DATA_COLUMN_ALIAS_PREFIX)) {
                if (columnName.endsWith("_" + ID_COLUMN)) {
                    subId = resultSet.getObject(i);
                } else if (columnName.endsWith("_" + TYPE_ID_COLUMN)) {
                    subTypeId = resultSet.getObject(i);
                } else if (columnName.endsWith("_" + DATA_COLUMN)) {
                    subData = resultSet.getBytes(i);
                    if (subId != null && subTypeId != null && subData != null && !subId.equals(objectState.getId())) {
                        Object subObject = createSavedObject(subTypeId, subId, query);
                        State subObjectState = State.getInstance(subObject);
                        subObjectState.setValues(unserializeData(subData));
                        subObject = swapObjectType(null, subObject);
                        subId = null;
                        subTypeId = null;
                        subData = null;
                        objectState.getExtras().put(State.SUB_DATA_STATE_EXTRA_PREFIX + subObjectState.getId(), subObject);
                    }
                }
            } else if (query.getExtraSourceColumns().containsKey(columnName)) {
                objectState.put(query.getExtraSourceColumns().get(columnName), resultSet.getObject(i));
            }
        }

        return swapObjectType(query, object);
    }

    /**
     * Executes the given read {@code statement} (created from the given
     * {@code sqlQuery}) before the given {@code timeout} (in seconds).
     */
    public ResultSet executeQueryBeforeTimeout(
            Statement statement,
            String sqlQuery,
            int timeout)
            throws SQLException {

        if (timeout > 0 && !(vendor instanceof SqlVendor.PostgreSQL)) {
            statement.setQueryTimeout(timeout);
        }

        Stats.Timer timer = STATS.startTimer();
        Profiler.Static.startThreadEvent(QUERY_PROFILER_EVENT);

        try {
            return statement.executeQuery(sqlQuery);

        } finally {
            double duration = timer.stop(QUERY_STATS_OPERATION);
            Profiler.Static.stopThreadEvent(sqlQuery);

            LOGGER.debug(
                    "Read from the SQL database using [{}] in [{}]ms",
                    sqlQuery, duration * 1000.0);
        }
    }

    /**
     * Selects the first object that matches the given {@code sqlQuery}
     * with options from the given {@code query}.
     */
    public <T> T selectFirstWithOptions(String sqlQuery, Query<T> query) {
        sqlQuery = vendor.rewriteQueryWithLimitClause(sqlQuery, 1, 0);
        ConnectionRef extraConnectionRef = new ConnectionRef();
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));
            return result.next() ? createSavedObjectWithResultSet(result, query, extraConnectionRef) : null;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
            extraConnectionRef.close();
        }
    }

    /**
     * Selects the first object that matches the given {@code sqlQuery}
     * without a timeout.
     */
    public Object selectFirst(String sqlQuery) {
        return selectFirstWithOptions(sqlQuery, null);
    }

    /**
     * Selects a list of objects that match the given {@code sqlQuery}
     * with options from the given {@code query}.
     */
    public <T> List<T> selectListWithOptions(String sqlQuery, Query<T> query) {
        ConnectionRef extraConnectionRef = new ConnectionRef();
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;
        List<T> objects = new ArrayList<>();
        int timeout = getQueryReadTimeout(query);

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, timeout);
            while (result.next()) {
                objects.add(createSavedObjectWithResultSet(result, query, extraConnectionRef));
            }

            return objects;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
            extraConnectionRef.close();
        }
    }

    /**
     * Selects a list of objects that match the given {@code sqlQuery}
     * without a timeout.
     */
    public List<Object> selectList(String sqlQuery) {
        return selectListWithOptions(sqlQuery, null);
    }

    /**
     * Returns an iterable that selects all objects matching the given
     * {@code sqlQuery} with options from the given {@code query}.
     */
    public <T> Iterable<T> selectIterableWithOptions(String sqlQuery, int fetchSize, Query<T> query) {
        return () -> new SqlIterator<>(sqlQuery, fetchSize, query);
    }

    private class SqlIterator<T> implements java.io.Closeable, Iterator<T> {

        private final String sqlQuery;
        private final Query<T> query;
        private final ConnectionRef extraConnectionRef;

        private final Connection connection;
        private final Statement statement;
        private final ResultSet result;

        private boolean hasNext = true;

        public SqlIterator(String initialSqlQuery, int fetchSize, Query<T> initialQuery) {
            sqlQuery = initialSqlQuery;
            query = initialQuery;
            extraConnectionRef = new ConnectionRef();

            try {
                connection = openQueryConnection(query);
                statement = connection.createStatement();
                statement.setFetchSize(getVendor() instanceof SqlVendor.MySQL ? Integer.MIN_VALUE
                        : fetchSize <= 0 ? 200
                        : fetchSize);
                result = statement.executeQuery(sqlQuery);
                moveToNext();

            } catch (SQLException ex) {
                close();
                throw createQueryException(ex, sqlQuery, query);
            }
        }

        private void moveToNext() throws SQLException {
            if (hasNext) {
                hasNext = result.next();
                if (!hasNext) {
                    close();
                }
            }
        }

        @Override
        public void close() {
            hasNext = false;
            closeResources(query, connection, statement, result);
            extraConnectionRef.close();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }

            try {
                T object = createSavedObjectWithResultSet(result, query, extraConnectionRef);
                moveToNext();
                return object;

            } catch (SQLException ex) {
                close();
                throw createQueryException(ex, sqlQuery, query);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            close();
        }
    }

    // --- AbstractDatabase support ---

    @Override
    public Connection openConnection() {
        DataSource dataSource = getDataSource();

        if (dataSource == null) {
            throw new SqlDatabaseException(this, "No SQL data source!");
        }

        try {
            Connection connection = getConnectionFromDataSource(dataSource);
            SqlSchema schema = schema();

            connection.setReadOnly(false);

            if (schema != null) {
                schema.setTransactionIsolation(connection);
            }

            return connection;

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't connect to the SQL engine!", error);
        }
    }

    private Connection getConnectionFromDataSource(DataSource dataSource) throws SQLException {
        int limit = Settings.getOrDefault(int.class, "dari/sqlConnectionRetryLimit", 5);

        while (true) {
            try {
                Connection connection = dataSource.getConnection();
                String catalog = getCatalog();

                if (catalog != null) {
                    connection.setCatalog(catalog);
                }

                return connection;

            } catch (SQLException error) {
                if (error instanceof SQLRecoverableException) {
                    -- limit;

                    if (limit >= 0) {
                        Stats.Timer timer = STATS.startTimer();

                        try {
                            Thread.sleep(ObjectUtils.jitter(10L, 0.5));

                        } catch (InterruptedException ignore) {
                            // Ignore and keep retrying.

                        } finally {
                            timer.stop(CONNECTION_ERROR_STATS_OPERATION);
                        }

                        continue;
                    }
                }

                throw error;
            }
        }
    }

    @Override
    protected Connection doOpenReadConnection() {
        DataSource readDataSource = getReadDataSource();

        if (readDataSource == null) {
            readDataSource = getDataSource();
        }

        if (readDataSource == null) {
            throw new SqlDatabaseException(this, "No SQL data source!");
        }

        try {
            Connection connection = getConnectionFromDataSource(readDataSource);

            connection.setReadOnly(true);
            return connection;

        } catch (SQLException error) {
            throw new SqlDatabaseException(this, "Can't connect to the SQL engine!", error);
        }
    }

    @Override
    public Connection openQueryConnection(Query<?> query) {
        if (query != null) {
            Connection connection = (Connection) query.getOptions().get(CONNECTION_QUERY_OPTION);

            if (connection != null) {
                return connection;
            }

            Boolean useRead = ObjectUtils.to(Boolean.class, query.getOptions().get(USE_READ_DATA_SOURCE_QUERY_OPTION));

            if (useRead == null) {
                useRead = Boolean.TRUE;
            }

            if (!useRead) {
                return openConnection();
            }
        }

        return super.openQueryConnection(query);
    }

    @Override
    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                if (defaultCatalog != null) {
                    String catalog = getCatalog();

                    if (catalog != null) {
                        connection.setCatalog(defaultCatalog);
                    }
                }

                if (!connection.isClosed()) {
                    connection.close();
                }

            } catch (SQLException error) {
                // Not likely and probably harmless.
            }
        }
    }

    @Override
    protected boolean isRecoverableError(Exception error) {
        if (error instanceof SQLException) {
            SQLException sqlError = (SQLException) error;
            return "40001".equals(sqlError.getSQLState());
        }

        return false;
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        close();
        setReadDataSource(createDataSource(
                settings,
                READ_DATA_SOURCE_SETTING,
                READ_DATA_SOURCE_JNDI_NAME_SETTING,
                READ_JDBC_DRIVER_CLASS_SETTING,
                READ_JDBC_URL_SETTING,
                READ_JDBC_USER_SETTING,
                READ_JDBC_PASSWORD_SETTING,
                READ_JDBC_POOL_SIZE_SETTING));
        setDataSource(createDataSource(
                settings,
                DATA_SOURCE_SETTING,
                DATA_SOURCE_JNDI_NAME_SETTING,
                JDBC_DRIVER_CLASS_SETTING,
                JDBC_URL_SETTING,
                JDBC_USER_SETTING,
                JDBC_PASSWORD_SETTING,
                JDBC_POOL_SIZE_SETTING));

        setCatalog(ObjectUtils.to(String.class, settings.get(CATALOG_SUB_SETTING)));

        setMetricCatalog(ObjectUtils.to(String.class, settings.get(METRIC_CATALOG_SUB_SETTING)));

        String vendorClassName = ObjectUtils.to(String.class, settings.get(VENDOR_CLASS_SETTING));
        Class<?> vendorClass = null;

        if (vendorClassName != null) {
            vendorClass = ObjectUtils.getClassByName(vendorClassName);
            if (vendorClass == null) {
                throw new SettingsException(
                        VENDOR_CLASS_SETTING,
                        String.format("Can't find [%s]!",
                        vendorClassName));
            } else if (!SqlVendor.class.isAssignableFrom(vendorClass)) {
                throw new SettingsException(
                        VENDOR_CLASS_SETTING,
                        String.format("[%s] doesn't implement [%s]!",
                        vendorClass, Driver.class));
            }
        }

        if (vendorClass != null) {
            setVendor((SqlVendor) TypeDefinition.getInstance(vendorClass).newInstance());
        }

        Boolean compressData = ObjectUtils.firstNonNull(
                ObjectUtils.to(Boolean.class, settings.get(COMPRESS_DATA_SUB_SETTING)),
                Settings.get(Boolean.class, "dari/isCompressSqlData"));
        if (compressData != null) {
            setCompressData(compressData);
        }

        setIndexSpatial(ObjectUtils.firstNonNull(ObjectUtils.to(Boolean.class, settings.get(INDEX_SPATIAL_SUB_SETTING)), Boolean.TRUE));
    }

    private static final Map<String, String> DRIVER_CLASS_NAMES; static {
        Map<String, String> m = new HashMap<>();
        m.put("h2", "org.h2.Driver");
        m.put("jtds", "net.sourceforge.jtds.jdbc.Driver");
        m.put("mysql", "com.mysql.jdbc.Driver");
        m.put("postgresql", "org.postgresql.Driver");
        DRIVER_CLASS_NAMES = m;
    }

    private static final Set<WeakReference<Driver>> REGISTERED_DRIVERS = new HashSet<>();

    private DataSource createDataSource(
            Map<String, Object> settings,
            String dataSourceSetting,
            String dataSourceJndiNameSetting,
            String jdbcDriverClassSetting,
            String jdbcUrlSetting,
            String jdbcUserSetting,
            String jdbcPasswordSetting,
            String jdbcPoolSizeSetting) {

        Object dataSourceJndiName = settings.get(dataSourceJndiNameSetting);
        if (dataSourceJndiName instanceof String) {
            try {
                Object dataSourceObject = new InitialContext().lookup((String) dataSourceJndiName);
                if (dataSourceObject instanceof DataSource) {
                    return (DataSource) dataSourceObject;
                }
            } catch (NamingException e) {
                throw new SettingsException(dataSourceJndiNameSetting,
                        String.format("Can't find [%s]!",
                        dataSourceJndiName), e);
            }
        }

        Object dataSourceObject = settings.get(dataSourceSetting);
        if (dataSourceObject instanceof DataSource) {
            return (DataSource) dataSourceObject;

        } else {
            String url = ObjectUtils.to(String.class, settings.get(jdbcUrlSetting));
            if (ObjectUtils.isBlank(url)) {
                return null;

            } else {
                String driverClassName = ObjectUtils.to(String.class, settings.get(jdbcDriverClassSetting));
                Class<?> driverClass = null;

                if (driverClassName != null) {
                    driverClass = ObjectUtils.getClassByName(driverClassName);
                    if (driverClass == null) {
                        throw new SettingsException(
                                jdbcDriverClassSetting,
                                String.format("Can't find [%s]!",
                                driverClassName));
                    } else if (!Driver.class.isAssignableFrom(driverClass)) {
                        throw new SettingsException(
                                jdbcDriverClassSetting,
                                String.format("[%s] doesn't implement [%s]!",
                                driverClass, Driver.class));
                    }

                } else {
                    int firstColonAt = url.indexOf(':');
                    if (firstColonAt > -1) {
                        ++ firstColonAt;
                        int secondColonAt = url.indexOf(':', firstColonAt);
                        if (secondColonAt > -1) {
                            driverClass = ObjectUtils.getClassByName(DRIVER_CLASS_NAMES.get(url.substring(firstColonAt, secondColonAt)));
                        }
                    }
                }

                if (driverClass != null) {
                    Driver driver = null;
                    for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements();) {
                        Driver d = e.nextElement();
                        if (driverClass.isInstance(d)) {
                            driver = d;
                            break;
                        }
                    }

                    if (driver == null) {
                        driver = (Driver) TypeDefinition.getInstance(driverClass).newInstance();
                        try {
                            LOGGER.info("Registering [{}]", driver);
                            DriverManager.registerDriver(driver);
                        } catch (SQLException ex) {
                            LOGGER.warn("Can't register [{}]!", driver);
                        }
                    }

                    REGISTERED_DRIVERS.add(new WeakReference<>(driver));
                }

                String user = ObjectUtils.to(String.class, settings.get(jdbcUserSetting));
                String password = ObjectUtils.to(String.class, settings.get(jdbcPasswordSetting));

                Integer poolSize = ObjectUtils.to(Integer.class, settings.get(jdbcPoolSizeSetting));
                if (poolSize == null || poolSize <= 0) {
                    poolSize = 24;
                }

                LOGGER.info(
                        "Automatically creating connection pool:"
                                + "\n\turl={}"
                                + "\n\tusername={}"
                                + "\n\tpoolSize={}",
                        new Object[] {
                                url,
                                user,
                                poolSize });

                HikariDataSource ds = new HikariDataSource();

                ds.setJdbcUrl(url);
                ds.setUsername(user);
                ds.setPassword(password);
                ds.setMaximumPoolSize(poolSize);

                return ds;
            }
        }
    }

    /** Returns the read timeout associated with the given {@code query}. */
    private int getQueryReadTimeout(Query<?> query) {
        if (query != null) {
            Double timeout = query.getTimeout();
            if (timeout == null) {
                timeout = getReadTimeout();
            }
            if (timeout > 0.0) {
                return (int) Math.round(timeout);
            }
        }
        return 0;
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        return selectListWithOptions(buildSelectStatement(query), query);
    }

    @Override
    public long readCount(Query<?> query) {
        String sqlQuery = buildCountStatement(query);
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

            if (result.next()) {
                Object countObj = result.getObject(1);
                if (countObj instanceof Number) {
                    return ((Number) countObj).longValue();
                }
            }

            return 0;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
        }
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        return selectFirstWithOptions(buildSelectStatement(query), query);
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        Boolean useJdbc = ObjectUtils.to(Boolean.class, query.getOptions().get(USE_JDBC_FETCH_SIZE_QUERY_OPTION));
        if (useJdbc == null) {
            useJdbc = Boolean.TRUE;
        }
        if (useJdbc) {
            return selectIterableWithOptions(buildSelectStatement(query), fetchSize, query);
        } else {
            return new ByIdIterable<>(query, fetchSize);
        }
    }

    private static class ByIdIterable<T> implements Iterable<T> {

        private final Query<T> query;
        private final int fetchSize;

        public ByIdIterable(Query<T> query, int fetchSize) {
            this.query = query;
            this.fetchSize = fetchSize;
        }

        @Override
        public Iterator<T> iterator() {
            return new ByIdIterator<>(query, fetchSize);
        }
    }

    private static class ByIdIterator<T> implements Iterator<T> {

        private final Query<T> query;
        private final int fetchSize;
        private UUID lastTypeId;
        private UUID lastId;
        private List<T> items;
        private int index;

        public ByIdIterator(Query<T> query, int fetchSize) {
            if (!query.getSorters().isEmpty()) {
                throw new IllegalArgumentException("Can't iterate over a query that has sorters!");
            }

            this.query = query.clone().timeout(0.0).sortAscending("_type").sortAscending("_id");
            this.fetchSize = fetchSize > 0 ? fetchSize : 200;
        }

        @Override
        public boolean hasNext() {
            if (items != null && items.isEmpty()) {
                return false;
            }

            if (items == null || index >= items.size()) {
                Query<T> nextQuery = query.clone();
                if (lastTypeId != null) {
                    nextQuery.and("_type = ? and _id > ?", lastTypeId, lastId);
                }

                items = nextQuery.select(0, fetchSize).getItems();

                int size = items.size();
                if (size < 1) {
                    if (lastTypeId == null) {
                        return false;

                    } else {
                        nextQuery = query.clone().and("_type > ?", lastTypeId);
                        items = nextQuery.select(0, fetchSize).getItems();
                        size = items.size();

                        if (size < 1) {
                            return false;
                        }
                    }
                }

                State lastState = State.getInstance(items.get(size - 1));
                lastTypeId = lastState.getVisibilityAwareTypeId();
                lastId = lastState.getId();
                index = 0;
            }

            return true;
        }

        @Override
        public T next() {
            if (hasNext()) {
                T object = items.get(index);
                ++ index;
                return object;

            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        String sqlQuery = buildLastUpdateStatement(query);
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

            if (result.next()) {
                Double date = ObjectUtils.to(Double.class, result.getObject(1));
                if (date != null) {
                    return new Date((long) (date * 1000L));
                }
            }

            return null;

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
        }
    }

    @Override
    public <T> PaginatedResult<T> readPartial(final Query<T> query, long offset, int limit) {
        // Guard against integer overflow
        if (limit == Integer.MAX_VALUE) {
            limit --;
        }
        List<T> objects = selectListWithOptions(
                vendor.rewriteQueryWithLimitClause(buildSelectStatement(query), limit + 1, offset),
                query);

        int size = objects.size();
        if (size <= limit) {
            return new PaginatedResult<>(offset, limit, offset + size, objects);

        } else {
            objects.remove(size - 1);
            return new PaginatedResult<T>(offset, limit, 0, objects) {

                private Long count;

                @Override
                public long getCount() {
                    if (count == null) {
                        count = readCount(query);
                    }
                    return count;
                }

                @Override
                public boolean hasNext() {
                    return true;
                }
            };
        }
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        for (String field : fields) {
            Matcher groupingMatcher = Query.RANGE_PATTERN.matcher(field);
            if (groupingMatcher.find()) {
                throw new UnsupportedOperationException("SqlDatabase does not support group by numeric range");
            }
        }

        List<Grouping<T>> groupings = new ArrayList<>();
        String sqlQuery = buildGroupStatement(query, fields);
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            connection = openQueryConnection(query);
            statement = connection.createStatement();
            result = executeQueryBeforeTimeout(statement, sqlQuery, getQueryReadTimeout(query));

            int fieldsLength = fields.length;
            int groupingsCount = 0;

            for (int i = 0, last = (int) offset + limit; result.next(); ++ i, ++ groupingsCount) {
                if (i < offset || i >= last) {
                    continue;
                }

                List<Object> keys = new ArrayList<>();

                SqlGrouping<T> grouping;
                ResultSetMetaData meta = result.getMetaData();
                String aggregateColumnName = meta.getColumnName(1);
                if (SqlQuery.COUNT_ALIAS.equals(aggregateColumnName)) {
                    long count = ObjectUtils.to(long.class, result.getObject(1));
                    for (int j = 0; j < fieldsLength; ++ j) {
                        keys.add(result.getObject(j + 2));
                    }
                    grouping = new SqlGrouping<>(keys, query, fields, count, groupings);
                } else {
                    throw new UnsupportedOperationException();
                }
                groupings.add(grouping);
            }

            int groupingsSize = groupings.size();
            List<Integer> removes = new ArrayList<>();

            for (int i = 0; i < fieldsLength; ++ i) {
                Query.MappedKey key = query.mapEmbeddedKey(getEnvironment(), fields[i]);
                ObjectField field = key.getSubQueryKeyField();
                if (field == null) {
                    field = key.getField();
                }

                if (field != null) {
                    Map<String, Object> rawKeys = new HashMap<>();
                    for (int j = 0; j < groupingsSize; ++ j) {
                        rawKeys.put(String.valueOf(j), groupings.get(j).getKeys().get(i));
                    }

                    String itemType = field.getInternalItemType();
                    if (ObjectField.RECORD_TYPE.equals(itemType)) {
                        for (Map.Entry<String, Object> entry : rawKeys.entrySet()) {
                            Map<String, Object> ref = new HashMap<>();
                            ref.put(StateValueUtils.REFERENCE_KEY, entry.getValue());
                            entry.setValue(ref);
                        }
                    }

                    Map<String, Object> rawKeysCopy = new HashMap<>(rawKeys);
                    Map<?, ?> convertedKeys = (Map<?, ?>) StateValueUtils.toJavaValue(query.getDatabase(), null, field, "map/" + itemType, rawKeys);

                    for (int j = 0; j < groupingsSize; ++ j) {
                        String jString = String.valueOf(j);
                        Object convertedKey = convertedKeys.get(jString);

                        if (convertedKey == null
                                && rawKeysCopy.get(jString) != null) {
                            removes.add(j - removes.size());
                        }

                        groupings.get(j).getKeys().set(i, convertedKey);
                    }
                }
            }

            for (Integer i : removes) {
                groupings.remove((int) i);
            }

            return new PaginatedResult<>(offset, limit, groupingsCount - removes.size(), groupings);

        } catch (SQLException ex) {
            throw createQueryException(ex, sqlQuery, query);

        } finally {
            closeResources(query, connection, statement, result);
        }
    }

    /** SQL-specific implementation of {@link Grouping}. */
    private class SqlGrouping<T> extends AbstractGrouping<T> {

        private final long count;
        private final List<Grouping<T>> groupings;

        public SqlGrouping(List<Object> keys, Query<T> query, String[] fields, long count, List<Grouping<T>> groupings) {
            super(keys, query, fields);
            this.count = count;
            this.groupings = groupings;
        }

        // --- AbstractGrouping support ---

        @Override
        protected Aggregate createAggregate(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    @Override
    protected void beginTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.setAutoCommit(false);
    }

    @Override
    protected void commitTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.commit();
    }

    @Override
    protected void rollbackTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.rollback();
    }

    @Override
    protected void endTransaction(Connection connection, boolean isImmediate) throws SQLException {
        connection.setAutoCommit(true);
    }

    /**
     * Returns {@code true} if the writes should use {@link Savepoint}s.
     */
    protected boolean shouldUseSavepoint() {
        return true;
    }

    private int execute(Connection connection, DSLContext context, org.jooq.Query query) throws SQLException {
        boolean useSavepoint = shouldUseSavepoint();
        Savepoint savepoint = null;
        Integer affected = null;

        Stats.Timer timer = STATS.startTimer();
        Profiler.Static.startThreadEvent(UPDATE_PROFILER_EVENT);

        try {
            if (useSavepoint && !connection.getAutoCommit()) {
                savepoint = connection.setSavepoint();
            }

            affected = query.execute();

            if (savepoint != null) {
                connection.releaseSavepoint(savepoint);
            }

            return affected;

        } catch (DataAccessException error) {
            if (savepoint != null) {
                connection.rollback(savepoint);
            }

            Throwables.propagateIfInstanceOf(error.getCause(), SQLException.class);
            throw error;

        } finally {
            double time = timer.stop(UPDATE_STATS_OPERATION);
            java.util.function.Supplier<String> sqlSupplier = () -> context
                    .renderContext()
                    .paramType(ParamType.INLINED)
                    .render(query);

            Profiler.Static.stopThreadEvent(sqlSupplier);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "SQL update: [{}], Affected: [{}], Time: [{}]ms",
                        new Object[] {
                                sqlSupplier.get(),
                                affected,
                                time * 1000.0
                        });
            }
        }
    }

    @Override
    protected void doSaves(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        List<State> indexStates = null;
        for (State state1 : states) {
            if (Boolean.TRUE.equals(state1.getExtra(SKIP_INDEX_STATE_EXTRA))) {
                indexStates = new ArrayList<>();
                for (State state2 : states) {
                    if (!Boolean.TRUE.equals(state2.getExtra(SKIP_INDEX_STATE_EXTRA))) {
                        indexStates.add(state2);
                    }
                }
                break;
            }
        }

        if (indexStates == null) {
            indexStates = states;
        }

        try (DSLContext context = openContext(connection)) {
            SqlSchema schema = schema();

            // Save all indexes.
            schema.deleteIndexes(this, connection, context, null, indexStates);
            schema.insertIndexes(this, connection, context, null, indexStates);

            double now = System.currentTimeMillis() / 1000.0;

            for (State state : states) {
                boolean isNew = state.isNew();
                UUID id = state.getId();
                UUID typeId = state.getVisibilityAwareTypeId();
                byte[] data = null;

                // Save data.
                while (true) {

                    // Looks like a new object so try to INSERT.
                    if (isNew) {
                        try {
                            if (data == null) {
                                data = serializeState(state);
                            }

                            execute(connection, context, context
                                    .insertInto(schema.recordTable())
                                    .set(schema.recordIdField(), id)
                                    .set(schema.recordTypeIdField(), typeId)
                                    .set(schema.recordDataField(), data));

                        } catch (SQLException error) {

                            // INSERT failed so retry with UPDATE.
                            if (Static.isIntegrityConstraintViolation(error)) {
                                isNew = false;
                                continue;

                            } else {
                                throw error;
                            }
                        }

                    } else {
                        List<AtomicOperation> atomicOperations = state.getAtomicOperations();

                        // Normal update.
                        if (atomicOperations.isEmpty()) {
                            if (data == null) {
                                data = serializeState(state);
                            }

                            if (execute(connection, context, context
                                    .update(schema.recordTable())
                                    .set(schema.recordTypeIdField(), typeId)
                                    .set(schema.recordDataField(), data)
                                    .where(schema.recordIdField().eq(id))) < 1) {

                                // UPDATE failed so retry with INSERT.
                                isNew = true;
                                continue;
                            }

                        } else {

                            // Atomic operations requested, so find the old object.
                            Object oldObject = Query
                                    .from(Object.class)
                                    .where("_id = ?", id)
                                    .using(this)
                                    .option(CONNECTION_QUERY_OPTION, connection)
                                    .option(RETURN_ORIGINAL_DATA_QUERY_OPTION, Boolean.TRUE)
                                    .option(USE_READ_DATA_SOURCE_QUERY_OPTION, Boolean.FALSE)
                                    .first();

                            if (oldObject == null) {
                                retryWrites();
                                break;
                            }

                            // Restore the data from the old object.
                            State oldState = State.getInstance(oldObject);
                            UUID oldTypeId = oldState.getVisibilityAwareTypeId();
                            byte[] oldData = Static.getOriginalData(oldObject);

                            state.setValues(oldState.getValues());

                            // Apply all the atomic operations.
                            for (AtomicOperation operation : atomicOperations) {
                                String field = operation.getField();
                                state.putByPath(field, oldState.getByPath(field));
                            }

                            for (AtomicOperation operation : atomicOperations) {
                                operation.execute(state);
                            }

                            data = serializeState(state);

                            if (execute(connection, context, context
                                    .update(schema.recordTable())
                                    .set(schema.recordTypeIdField(), typeId)
                                    .set(schema.recordDataField(), data)
                                    .where(schema.recordIdField().eq(id))
                                    .and(schema.recordTypeIdField().eq(oldTypeId))
                                    .and(schema.recordDataField().eq(oldData))) < 1) {

                                // UPDATE failed so start over.
                                retryWrites();
                                break;
                            }
                        }
                    }

                    // Success!
                    break;
                }

                // Save update date.
                while (true) {
                    if (isNew) {
                        try {
                            execute(connection, context, context
                                    .insertInto(schema.recordUpdateTable())
                                    .set(schema.recordUpdateIdField(), id)
                                    .set(schema.recordUpdateTypeIdField(), typeId)
                                    .set(schema.recordUpdateDateField(), now));

                        } catch (SQLException error) {

                            // INSERT failed so retry with UPDATE.
                            if (Static.isIntegrityConstraintViolation(error)) {
                                isNew = false;
                                continue;

                            } else {
                                throw error;
                            }
                        }

                    } else {
                        if (execute(connection, context, context
                                .update(schema.recordUpdateTable())
                                .set(schema.recordUpdateTypeIdField(), typeId)
                                .set(schema.recordUpdateDateField(), now)
                                .where(schema.recordUpdateIdField().eq(id))) < 1) {

                            // UPDATE failed so retry with INSERT.
                            isNew = true;
                            continue;
                        }
                    }

                    break;
                }
            }
        }
    }

    @Override
    protected void doIndexes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        try (DSLContext context = openContext(connection)) {
            SqlSchema schema = schema();

            schema.deleteIndexes(this, connection, context, null, states);
            schema.insertIndexes(this, connection, context, null, states);
        }
    }

    @Override
    public void doRecalculations(Connection connection, boolean isImmediate, ObjectIndex index, List<State> states) throws SQLException {
        try (DSLContext context = openContext(connection)) {
            SqlSchema schema = schema();

            schema.deleteIndexes(this, connection, context, index, states);
            schema.insertIndexes(this, connection, context, index, states);
        }
    }

    @Override
    protected void doDeletes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        try (DSLContext context = openContext(connection)) {
            SqlSchema schema = schema();

            // Delete all indexes.
            schema.deleteIndexes(this, connection, context, null, states);

            Set<UUID> stateIds = states.stream()
                    .map(State::getId)
                    .collect(Collectors.toSet());

            // Delete data.
            execute(connection, context, context
                    .delete(schema.recordTable())
                    .where(schema.recordIdField().in(stateIds)));

            // Save delete date.
            execute(connection, context, context
                    .update(schema.recordUpdateTable())
                    .set(schema.recordUpdateDateField(), System.currentTimeMillis() / 1000.0)
                    .where(schema.recordUpdateIdField().in(stateIds)));
        }
    }

    @Override
    public void addUpdateNotifier(UpdateNotifier<?> notifier) {
        updateNotifiers.add(notifier);
    }

    @Override
    public void removeUpdateNotifier(UpdateNotifier<?> notifier) {
        updateNotifiers.remove(notifier);
    }

    public void notifyUpdate(Object object) {
        NOTIFIER: for (UpdateNotifier<?> notifier : updateNotifiers) {
            for (Type notifierInterface : notifier.getClass().getGenericInterfaces()) {
                if (notifierInterface instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) notifierInterface;
                    Type rt = pt.getRawType();

                    if (rt instanceof Class
                            && UpdateNotifier.class.isAssignableFrom((Class<?>) rt)) {

                        Type[] args = pt.getActualTypeArguments();

                        if (args.length > 0) {
                            Type arg = args[0];

                            if (arg instanceof Class
                                    && !((Class<?>) arg).isInstance(object)) {
                                continue NOTIFIER;

                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            @SuppressWarnings("unchecked")
            UpdateNotifier<Object> objectNotifier = (UpdateNotifier<Object>) notifier;

            try {
                objectNotifier.onUpdate(object);

            } catch (Exception error) {
                LOGGER.warn(
                        String.format(
                                "Can't notify [%s] of [%s] update!",
                                notifier,
                                State.getInstance(object).getId()),
                        error);
            }
        }
    }

    /** {@link AbstractSqlDatabase} utility methods. */
    public static final class Static {

        public static List<AbstractSqlDatabase> getAll() {
            return INSTANCES;
        }

        public static void deregisterAllDrivers() {
            for (WeakReference<Driver> driverRef : REGISTERED_DRIVERS) {
                Driver driver = driverRef.get();
                if (driver != null) {
                    LOGGER.info("Deregistering [{}]", driver);
                    try {
                        DriverManager.deregisterDriver(driver);
                    } catch (SQLException ex) {
                        LOGGER.warn("Can't deregister [{}]!", driver);
                    }
                }
            }
        }

        /**
         * Returns {@code true} if the given {@code error} looks like a
         * {@link SQLIntegrityConstraintViolationException}.
         */
        public static boolean isIntegrityConstraintViolation(SQLException error) {
            if (error instanceof SQLIntegrityConstraintViolationException) {
                return true;
            } else {
                String state = error.getSQLState();
                return state != null && state.startsWith("23");
            }
        }

        public static byte[] getOriginalData(Object object) {
            return (byte[]) State.getInstance(object).getExtra(ORIGINAL_DATA_EXTRA);
        }
    }
}
