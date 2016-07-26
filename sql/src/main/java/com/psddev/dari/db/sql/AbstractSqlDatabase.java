package com.psddev.dari.db.sql;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import com.google.common.collect.ImmutableList;
import com.psddev.dari.db.AbstractDatabase;
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
import com.psddev.dari.util.IoUtils;
import com.zaxxer.hikari.HikariDataSource;
import org.iq80.snappy.Snappy;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.DeleteConditionStep;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
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
import com.psddev.dari.util.TypeDefinition;

/** Database backed by a SQL engine. */
public abstract class AbstractSqlDatabase extends AbstractDatabase<Connection> implements MetricSqlDatabase {

    public static final int MAX_STRING_INDEX_TYPE_LENGTH = 500;

    private static final DataType<String> STRING_INDEX_TYPE = SQLDataType.LONGVARBINARY.asConvertedDataType(new Converter<byte[], String>() {

        @Override
        public String from(byte[] bytes) {
            return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
        }

        @Override
        public byte[] to(String string) {
            if (string != null) {
                byte[] bytes = string.getBytes(StandardCharsets.UTF_8);

                if (bytes.length <= MAX_STRING_INDEX_TYPE_LENGTH) {
                    return bytes;

                } else {
                    byte[] shortened = new byte[MAX_STRING_INDEX_TYPE_LENGTH];
                    System.arraycopy(bytes, 0, shortened, 0, MAX_STRING_INDEX_TYPE_LENGTH);
                    return shortened;
                }

            } else {
                return null;
            }
        }

        @Override
        public Class<byte[]> fromType() {
            return byte[].class;
        }

        @Override
        public Class<String> toType() {
            return String.class;
        }
    });

    public static final String DATA_SOURCE_SETTING = "dataSource";
    public static final String DATA_SOURCE_JNDI_NAME_SETTING = "dataSourceJndiName";
    public static final String JDBC_DRIVER_CLASS_SETTING = "jdbcDriverClass";
    public static final String JDBC_URL_SETTING = "jdbcUrl";
    public static final String JDBC_USER_SETTING = "jdbcUser";
    public static final String JDBC_PASSWORD_SETTING = "jdbcPassword";

    public static final String READ_DATA_SOURCE_SETTING = "readDataSource";
    public static final String READ_DATA_SOURCE_JNDI_NAME_SETTING = "readDataSourceJndiName";
    public static final String READ_JDBC_DRIVER_CLASS_SETTING = "readJdbcDriverClass";
    public static final String READ_JDBC_URL_SETTING = "readJdbcUrl";
    public static final String READ_JDBC_USER_SETTING = "readJdbcUser";
    public static final String READ_JDBC_PASSWORD_SETTING = "readJdbcPassword";

    public static final String CATALOG_SUB_SETTING = "catalog";
    public static final String METRIC_CATALOG_SUB_SETTING = "metricCatalog";
    public static final String COMPRESS_DATA_SUB_SETTING = "compressData";

    public static final String INDEX_SPATIAL_SUB_SETTING = "indexSpatial";

    public static final String RECORD_TABLE = "Record";

    public static final String CONNECTION_QUERY_OPTION = "sql.connection";
    public static final String RETURN_ORIGINAL_DATA_QUERY_OPTION = "sql.returnOriginalData";
    public static final String DISABLE_BY_ID_ITERATOR_OPTION = "sql.disableByIdIterator";
    public static final String USE_READ_DATA_SOURCE_QUERY_OPTION = "sql.useReadDataSource";
    public static final String SKIP_INDEX_STATE_EXTRA = "sql.skipIndex";

    public static final String ORIGINAL_DATA_EXTRA = "sql.originalData";

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

    private final Table<Record> recordTable;
    private final Field<UUID> recordIdField;
    private final Field<UUID> recordTypeIdField;
    private final Field<byte[]> recordDataField;

    private final Table<Record> recordUpdateTable;
    private final Field<UUID> recordUpdateIdField;
    private final Field<UUID> recordUpdateTypeIdField;
    private final Field<Double> recordUpdateDateField;

    private final Table<Record> symbolTable;
    private final Field<Integer> symbolIdField;
    private final Field<String> symbolValueField;

    private volatile List<AbstractSqlIndex> locationSqlIndexes;
    private volatile List<AbstractSqlIndex> numberSqlIndexes;
    private volatile List<AbstractSqlIndex> regionSqlIndexes;
    private volatile List<AbstractSqlIndex> stringSqlIndexes;
    private volatile List<AbstractSqlIndex> uuidSqlIndexes;
    private volatile List<AbstractSqlIndex> deleteSqlIndexes;

    private final Set<WeakReference<Driver>> registeredDrivers = new HashSet<>();
    private volatile DataSource dataSource;
    private volatile DataSource readDataSource;
    private volatile String catalog;
    private volatile String metricCatalog;
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

    // Cache that stores all the symbol IDs.
    private final transient Lazy<Map<String, Integer>> symbolIds = new Lazy<Map<String, Integer>>() {

        @Override
        protected Map<String, Integer> create() {
            Connection connection = openConnection();

            try (DSLContext context = openContext(connection)) {
                ResultQuery<Record2<Integer, String>> query = context
                        .select(symbolIdField(), symbolValueField())
                        .from(symbolTable());

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

    /** Closes all resources used by all instances. */
    public static void closeAll() {
        INSTANCES.forEach(AbstractSqlDatabase::close);
        INSTANCES.clear();
    }

    protected AbstractSqlDatabase() {
        DataType<byte[]> byteArrayType = byteArrayType();
        DataType<Double> doubleType = doubleType();
        DataType<Integer> integerType = integerType();
        DataType<String> stringIndexType = stringIndexType();
        DataType<UUID> uuidType = uuidType();

        recordTable = DSL.table(DSL.name("Record"));
        recordIdField = DSL.field(DSL.name("id"), uuidType);
        recordTypeIdField = DSL.field(DSL.name("typeId"), uuidType);
        recordDataField = DSL.field(DSL.name("data"), byteArrayType);

        recordUpdateTable = DSL.table(DSL.name("RecordUpdate"));
        recordUpdateIdField = DSL.field(DSL.name("id"), uuidType);
        recordUpdateTypeIdField = DSL.field(DSL.name("typeId"), uuidType);
        recordUpdateDateField = DSL.field(DSL.name("updateDate"), doubleType);

        symbolTable = DSL.table(DSL.name("Symbol"));
        symbolIdField = DSL.field(DSL.name("symbolId"), integerType);
        symbolValueField = DSL.field(DSL.name("value"), stringIndexType);
    }

    public DataType<byte[]> byteArrayType() {
        return SQLDataType.LONGVARBINARY;
    }

    public DataType<Double> doubleType() {
        return SQLDataType.DOUBLE;
    }

    public DataType<Integer> integerType() {
        return SQLDataType.INTEGER;
    }

    public DataType<String> stringIndexType() {
        return STRING_INDEX_TYPE;
    }

    public DataType<UUID> uuidType() {
        return SQLDataType.UUID;
    }

    public Field<Double> stArea(Field<Object> field) {
        return DSL.field("ST_Area({0})", Double.class, field);
    }

    public Condition stContains(Field<Object> x, Field<Object> y) {
        return DSL.condition("ST_Contains({0}, {1})", x, y);
    }

    public Field<Object> stGeomFromText(Field<String> wkt) {
        return DSL.field("ST_GeomFromText({0})", wkt);
    }

    public Field<Double> stLength(Field<Object> field) {
        return DSL.field("ST_Length({0})", Double.class, field);
    }

    public Field<Object> stMakeLine(Field<Object> x, Field<Object> y) {
        return DSL.field("ST_MakeLine({0}, {1})", x, y);
    }

    public Table<Record> recordTable() {
        return recordTable;
    }

    public Field<UUID> recordIdField() {
        return recordIdField;
    }

    public Field<UUID> recordTypeIdField() {
        return recordTypeIdField;
    }

    public Field<byte[]> recordDataField() {
        return recordDataField;
    }

    public Table<Record> recordUpdateTable() {
        return recordUpdateTable;
    }

    public Field<UUID> recordUpdateIdField() {
        return recordUpdateIdField;
    }

    public Field<UUID> recordUpdateTypeIdField() {
        return recordUpdateTypeIdField;
    }

    public Field<Double> recordUpdateDateField() {
        return recordUpdateDateField;
    }

    public Table<Record> symbolTable() {
        return symbolTable;
    }

    public Field<Integer> symbolIdField() {
        return symbolIdField;
    }

    public Field<String> symbolValueField() {
        return symbolValueField;
    }

    /**
     * Sets up the given {@code database}.
     *
     * <p>This method should create all the necessary elements, such as tables,
     * that are required for proper operation.</p>
     *
     * <p>The default implementation executes all SQL statements from
     * the resource at {@link #setUpResourcePath()} and processes the errors
     * using {@link #catchSetUpError(SQLException)}.</p>
     *
     * @param database Can't be {@code null}.
     */
    public void setUp(AbstractSqlDatabase database) throws IOException, SQLException {
        String resourcePath = setUpResourcePath();

        if (resourcePath == null) {
            return;
        }

        Connection connection = database.openConnection();

        try (DSLContext context = database.openContext(connection)) {

            // Skip set-up if the Record table already exists.
            if (context.meta().getTables().stream()
                    .filter(t -> t.getName().equals(recordTable().getName()))
                    .findFirst()
                    .isPresent()) {

                return;
            }

            try (InputStream resourceInput = getClass().getResourceAsStream(resourcePath)) {
                for (String ddl : IoUtils.toString(resourceInput, StandardCharsets.UTF_8).trim().split("(?:\r\n?|\n){2,}")) {
                    try {
                        context.execute(ddl);

                    } catch (DataAccessException error) {
                        Throwables.propagateIfInstanceOf(error.getCause(), SQLException.class);
                        throw error;
                    }
                }
            }

        } finally {
            database.closeConnection(connection);
        }
    }

    /**
     * Returns the path to the resource that contains the SQL statements to
     * be executed during {@link #setUp(AbstractSqlDatabase)}.
     *
     * <p>The default implementation returns {@code null} to signal that
     * there's nothing to do.</p>
     *
     * @return May be {@code null}.
     */
    protected String setUpResourcePath() {
        return null;
    }

    /**
     * Catches the given {@code error} thrown in
     * {@link #setUp(AbstractSqlDatabase)} to be processed in vendor-specific way.
     *
     * <p>Typically, this is used to ignore errors when the underlying
     * database doesn't natively support a specific capability (e.g.
     * {@code CREATE TABLE IF NOT EXISTS}).</p>
     *
     * <p>The default implementation always rethrows the error.</p>
     *
     * @param error Can't be {@code null}.
     */
    protected void catchSetUpError(SQLException error) throws SQLException {
        throw error;
    }

    /**
     * Sets the most appropriate transaction isolation on the given
     * {@code connection} in preparation for writing to the database.
     *
     * <p>The default implementation sets it to READ COMMITTED.</p>
     *
     * @param connection Can't be {@code null}.
     */
    public void setTransactionIsolation(Connection connection) throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    /**
     * Finds the index table that should be used with SELECT SQL queries.
     *
     * @param type May be {@code null}.
     * @return Never {@code null}.
     */
    public AbstractSqlIndex findSelectIndexTable(String type) {
        List<AbstractSqlIndex> tables = findUpdateIndexTables(type);

        if (tables.isEmpty()) {
            throw new UnsupportedOperationException();

        } else {
            return tables.get(tables.size() - 1);
        }
    }

    private String indexType(ObjectIndex index) {
        List<String> fieldNames = index.getFields();
        ObjectField field = index.getParent().getField(fieldNames.get(0));

        return field != null ? field.getInternalItemType() : index.getType();
    }

    public AbstractSqlIndex findSelectIndexTable(ObjectIndex index) {
        return findSelectIndexTable(indexType(index));
    }

    /**
     * Finds all the index tables that should be used with UPDATE SQL queries.
     *
     * @param type May be {@code null}.
     * @return Never {@code null}.
     */
    public List<AbstractSqlIndex> findUpdateIndexTables(String type) {
        switch (type) {
            case ObjectField.RECORD_TYPE :
            case ObjectField.UUID_TYPE :
                return uuidSqlIndexes;

            case ObjectField.DATE_TYPE :
            case ObjectField.NUMBER_TYPE :
                return numberSqlIndexes;

            case ObjectField.LOCATION_TYPE :
                return locationSqlIndexes;

            case ObjectField.REGION_TYPE :
                return regionSqlIndexes;

            default :
                return stringSqlIndexes;
        }
    }

    public List<AbstractSqlIndex> findUpdateIndexTables(ObjectIndex index) {
        return findUpdateIndexTables(indexType(index));
    }

    /**
     * Inserts indexes associated with the given {@code states}.
     */
    public void insertIndexes(
            AbstractSqlDatabase database,
            Connection connection,
            DSLContext context,
            ObjectIndex onlyIndex,
            List<State> states)
            throws SQLException {

        Map<Table<Record>, BatchBindStep> batches = new HashMap<>();
        Map<Table<Record>, Set<Map<String, Object>>> bindValuesSets = new HashMap<>();

        for (State state : states) {
            UUID id = state.getId();
            UUID typeId = state.getVisibilityAwareTypeId();

            for (SqlIndexValue sqlIndexValue : SqlIndexValue.find(state)) {
                ObjectIndex index = sqlIndexValue.getIndex();

                if (onlyIndex != null && !onlyIndex.equals(index)) {
                    continue;
                }

                Object symbolId = database.getSymbolId(sqlIndexValue.getUniqueName());

                for (AbstractSqlIndex sqlIndex : findUpdateIndexTables(index)) {
                    Table<Record> table = sqlIndex.table();
                    BatchBindStep batch = batches.get(table);
                    Param<UUID> idParam = sqlIndex.idParam();
                    Param<UUID> typeIdParam = sqlIndex.typeIdParam();
                    Param<Integer> symbolIdParam = sqlIndex.symbolIdParam();

                    if (batch == null) {
                        batch = context.batch(context.insertInto(table)
                                .set(sqlIndex.idField(), idParam)
                                .set(sqlIndex.typeIdField(), typeIdParam)
                                .set(sqlIndex.symbolIdField(), symbolIdParam)
                                .set(sqlIndex.valueField(), sqlIndex.valueParam()));
                    }

                    boolean bound = false;

                    for (Object[] valuesArray : sqlIndexValue.getValuesArray()) {
                        Map<String, Object> bindValues = sqlIndex.valueBindValues(index, valuesArray[0]);

                        if (bindValues != null) {
                            bindValues.put(idParam.getName(), id);
                            bindValues.put(typeIdParam.getName(), typeId);
                            bindValues.put(symbolIdParam.getName(), symbolId);

                            Set<Map<String, Object>> bindValuesSet = bindValuesSets.get(table);

                            if (bindValuesSet == null) {
                                bindValuesSet = new HashSet<>();
                                bindValuesSets.put(table, bindValuesSet);
                            }

                            if (!bindValuesSet.contains(bindValues)) {
                                batch = batch.bind(bindValues);
                                bound = true;
                                bindValuesSet.add(bindValues);
                            }
                        }
                    }

                    if (bound) {
                        batches.put(table, batch);
                    }
                }
            }
        }

        for (BatchBindStep batch : batches.values()) {
            try {
                batch.execute();

            } catch (DataAccessException error) {
                Throwables.propagateIfInstanceOf(error.getCause(), SQLException.class);
                throw error;
            }
        }
    }

    /**
     * Deletes indexes associated with the given {@code states}.
     */
    public void deleteIndexes(
            AbstractSqlDatabase database,
            Connection connection,
            DSLContext context,
            ObjectIndex onlyIndex,
            List<State> states)
            throws SQLException {

        if (states == null || states.isEmpty()) {
            return;
        }

        Set<UUID> stateIds = states.stream()
                .map(State::getId)
                .collect(Collectors.toSet());

        for (AbstractSqlIndex sqlIndex : deleteSqlIndexes) {
            try {
                DeleteConditionStep<Record> delete = context
                        .deleteFrom(sqlIndex.table())
                        .where(sqlIndex.idField().in(stateIds));

                if (onlyIndex != null) {
                    delete = delete.and(sqlIndex.symbolIdField().eq(database.getReadSymbolId(onlyIndex.getUniqueName())));
                }

                context.execute(delete);

            } catch (DataAccessException error) {
                Throwables.propagateIfInstanceOf(error, SQLException.class);
                throw error;
            }
        }
    }

    /** Returns the JDBC data source used for general database operations. */
    public DataSource getDataSource() {
        return dataSource;
    }

    /** Sets the JDBC data source used for general database operations. */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;

        if (dataSource == null) {
            return;
        }

        synchronized (this) {
            try {
                setUp(this);
                invalidateCaches();

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
            setUp(this);
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
     *     <li>{@link #getSymbolId(String)}</li>
     *     <li>{@link #getReadSymbolId(String)}</li>
     * </ul>
     */
    public void invalidateCaches() {
        symbolIds.reset();
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
            Table<Record> symbolTable = symbolTable();
            Field<Integer> symbolIdField = symbolIdField();
            Field<String> symbolValueField = symbolValueField();

            if (create) {
                org.jooq.Query createQuery = context
                        .insertInto(symbolTable, symbolValueField)
                        .select(context
                                .select(DSL.inline(symbol, stringIndexType()))
                                .whereNotExists(context
                                        .selectOne()
                                        .from(symbolTable)
                                        .where(symbolValueField.eq(symbol))));

                try {
                    createQuery.execute();

                } catch (DataAccessException error) {
                    throw convertJooqError(error, createQuery);
                }
            }

            ResultQuery<Record1<Integer>> selectQuery = context
                    .select(symbolIdField)
                    .from(symbolTable)
                    .where(symbolValueField.eq(symbol));

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
            LOGGER.info("Closing data source: " + dataSource);
            ((HikariDataSource) dataSource).close();
        }

        setDataSource(null);

        DataSource readDataSource = getReadDataSource();

        if (readDataSource instanceof HikariDataSource) {
            LOGGER.info("Closing read data source: " + dataSource);
            ((HikariDataSource) readDataSource).close();
        }

        setReadDataSource(null);

        for (Iterator<WeakReference<Driver>> i = registeredDrivers.iterator(); i.hasNext();) {
            Driver driver = i.next().get();

            i.remove();

            if (driver != null) {
                LOGGER.info("Deregistering JDBC driver [{}]", driver);

                try {
                    DriverManager.deregisterDriver(driver);

                } catch (SQLException error) {
                    LOGGER.warn("Can't deregister JDBC driver [{}]!", driver);
                }
            }
        }
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

    // Creates a previously saved object using the given resultSet.
    <T> T createSavedObjectWithResultSet(ResultSet resultSet, Query<T> query) throws SQLException {
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

        if (timeout > 0) {
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

    private <R> R select(String sqlQuery, Query<?> query, SqlSelectFunction<R> selectFunction) {
        Connection connection = openQueryConnection(query);

        try {
            double timeout = getReadTimeout();

            if (query != null) {
                Double queryTimeout = query.getTimeout();

                if (queryTimeout != null) {
                    timeout = queryTimeout;
                }
            }

            try (Statement statement = connection.createStatement();
                    ResultSet result = executeQueryBeforeTimeout(
                            statement,
                            sqlQuery,
                            timeout > 0.0d ? (int) Math.ceil(timeout) : 0)) {

                return selectFunction.apply(result);
            }

        } catch (SQLException error) {
            throw createSelectError(sqlQuery, query, error);

        } finally {
            closeResources(query, connection, null, null);
        }
    }

    protected SqlDatabaseException createSelectError(String sqlQuery, Query<?> query, SQLException error) {
        String message;

        if (error instanceof SQLTimeoutException
                || ((message = error.getMessage()) != null
                && message.toLowerCase(Locale.ENGLISH).contains("timeout"))) {

            return new SqlDatabaseException.ReadTimeout(this, error, sqlQuery, query);

        } else {
            return new SqlDatabaseException(this, error, sqlQuery, query);
        }
    }

    /**
     * Selects the first object that matches the given {@code sqlQuery},
     * executed with the given {@code query} options.
     *
     * @param sqlQuery Can't be {@code null}.
     * @param query May be {@code null}.
     */
    public <T> T selectFirst(String sqlQuery, Query<T> query) {
        sqlQuery = DSL.using(dialect())
                .selectFrom(DSL.table("(" + sqlQuery + ")").as("q"))
                .offset(0)
                .limit(1)
                .getSQL(ParamType.INLINED);

        return select(sqlQuery, query, result -> result.next()
                ? createSavedObjectWithResultSet(result, query)
                : null);
    }

    /**
     * Selects a list of objects that match the given {@code sqlQuery},
     * executed with the given {@code query} options.
     *
     * @param sqlQuery Can't be {@code null}.
     * @param query May be {@code null}.
     */
    public <T> List<T> selectList(String sqlQuery, Query<T> query) {
        return select(sqlQuery, query, result -> {
            List<T> objects = new ArrayList<>();

            while (result.next()) {
                objects.add(createSavedObjectWithResultSet(result, query));
            }

            return objects;
        });
    }

    /**
     * Selects an iterable of objects that match the given {@code sqlQuery},
     * executed with the given {@code query} options.
     *
     * @param sqlQuery Can't be {@code null}.
     * @param fetchSize Number of objects to fetch at a time.
     * @param query May be {@code null}.
     */
    public <T> Iterable<T> selectIterable(String sqlQuery, int fetchSize, Query<T> query) {
        return SqlIterator.iterable(this, sqlQuery, fetchSize, query);
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

            connection.setReadOnly(false);
            setTransactionIsolation(connection);

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

        setDataSource(createDataSource(
                settings,
                DATA_SOURCE_JNDI_NAME_SETTING,
                DATA_SOURCE_SETTING,
                JDBC_DRIVER_CLASS_SETTING,
                JDBC_URL_SETTING,
                JDBC_USER_SETTING,
                JDBC_PASSWORD_SETTING));

        setReadDataSource(createDataSource(
                settings,
                READ_DATA_SOURCE_JNDI_NAME_SETTING,
                READ_DATA_SOURCE_SETTING,
                READ_JDBC_DRIVER_CLASS_SETTING,
                READ_JDBC_URL_SETTING,
                READ_JDBC_USER_SETTING,
                READ_JDBC_PASSWORD_SETTING));

        setCatalog(ObjectUtils.to(String.class, settings.get(CATALOG_SUB_SETTING)));

        setMetricCatalog(ObjectUtils.to(String.class, settings.get(METRIC_CATALOG_SUB_SETTING)));

        Boolean compressData = ObjectUtils.firstNonNull(
                ObjectUtils.to(Boolean.class, settings.get(COMPRESS_DATA_SUB_SETTING)),
                Settings.get(Boolean.class, "dari/isCompressSqlData"));
        if (compressData != null) {
            setCompressData(compressData);
        }

        setIndexSpatial(ObjectUtils.firstNonNull(ObjectUtils.to(Boolean.class, settings.get(INDEX_SPATIAL_SUB_SETTING)), Boolean.TRUE));

        Connection connection = openAnyConnection();
        Set<String> existingTables;

        try (DSLContext context = openContext(connection)) {
            existingTables = context.meta().getTables().stream()
                    .map(t -> t.getName().toLowerCase(Locale.ENGLISH))
                    .collect(Collectors.toSet());

        } catch (DataAccessException error) {
            throw convertJooqError(error, null);

        } finally {
            closeConnection(connection);
        }

        numberSqlIndexes = sqlIndexes(existingTables, new NumberSqlIndex(this, "RecordNumber", 3));
        stringSqlIndexes = sqlIndexes(existingTables, new StringSqlIndex(this, "RecordString", 4));
        uuidSqlIndexes = sqlIndexes(existingTables, new UuidSqlIndex(this, "RecordUuid", 3));

        if (isIndexSpatial()) {
            locationSqlIndexes = sqlIndexes(existingTables, new LocationSqlIndex(this, "RecordLocation", 3));
            regionSqlIndexes = sqlIndexes(existingTables, new RegionSqlIndex(this, "RecordRegion", 2));

        } else {
            locationSqlIndexes = Collections.emptyList();
            regionSqlIndexes = Collections.emptyList();
        }

        deleteSqlIndexes = ImmutableList.<AbstractSqlIndex>builder()
                .addAll(locationSqlIndexes)
                .addAll(numberSqlIndexes)
                .addAll(regionSqlIndexes)
                .addAll(stringSqlIndexes)
                .addAll(uuidSqlIndexes)
                .build();
    }

    private List<AbstractSqlIndex> sqlIndexes(Set<String> existingTables, AbstractSqlIndex... sqlIndexes) {
        ImmutableList.Builder<AbstractSqlIndex> builder = ImmutableList.builder();
        boolean empty = true;

        for (AbstractSqlIndex sqlIndex : sqlIndexes) {
            if (existingTables.contains(sqlIndex.table().getName().toLowerCase(Locale.ENGLISH))) {
                builder.add(sqlIndex);
                empty = false;
            }
        }

        if (empty) {
            int length = sqlIndexes.length;

            if (length > 0) {
                builder.add(sqlIndexes[length - 1]);
            }
        }

        return builder.build();
    }

    private DataSource createDataSource(
            Map<String, Object> settings,
            String dataSourceJndiNameSetting,
            String dataSourceSetting,
            String jdbcDriverClassSetting,
            String jdbcUrlSetting,
            String jdbcUserSetting,
            String jdbcPasswordSetting) {

        // Data source at a non-standard location?
        String dataSourceJndiName = ObjectUtils.to(String.class, settings.get(dataSourceJndiNameSetting));
        Object dataSourceObject = null;

        if (dataSourceJndiName != null) {
            try {
                dataSourceObject = new InitialContext().lookup(dataSourceJndiName);

            } catch (NamingException error) {
                throw new SettingsException(
                        dataSourceJndiNameSetting,
                        String.format("Can't find [%s] via JNDI!", dataSourceJndiName),
                        error);
            }
        }

        // Data source at a standard location?
        if (dataSourceObject == null) {
            dataSourceObject = settings.get(dataSourceSetting);
        }

        // Really a data source?
        if (dataSourceObject != null) {
            if (dataSourceObject instanceof DataSource) {
                return (DataSource) dataSourceObject;

            } else {
                throw new SettingsException(
                        dataSourceSetting,
                        String.format("[%s] isn't a data source!", dataSourceObject));
            }
        }

        // No data source so try to create one via JDBC settings.
        String url = ObjectUtils.to(String.class, settings.get(jdbcUrlSetting));

        if (ObjectUtils.isBlank(url)) {
            return null;
        }

        // If JDBC driver is specified, try to register it.
        String driverClassName = ObjectUtils.to(String.class, settings.get(jdbcDriverClassSetting));

        if (driverClassName != null) {
            Class<?> driverClass = ObjectUtils.getClassByName(driverClassName);

            if (driverClass == null) {
                throw new SettingsException(
                        jdbcDriverClassSetting,
                        String.format("Can't find [%s] class!", driverClassName));

            } else if (!Driver.class.isAssignableFrom(driverClass)) {
                throw new SettingsException(
                        jdbcDriverClassSetting,
                        String.format("[%s] doesn't implement [%s]!", driverClass, Driver.class));
            }

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
                    LOGGER.info("Registering JDBC driver [{}]", driver);
                    DriverManager.registerDriver(driver);
                    registeredDrivers.add(new WeakReference<>(driver));

                } catch (SQLException error) {
                    LOGGER.warn(String.format("Can't register JDBC driver [%s]!", driver), error);
                }
            }
        }

        // Create the data source.
        HikariDataSource hikari = new HikariDataSource();

        hikari.setJdbcUrl(url);
        hikari.setUsername(ObjectUtils.to(String.class, settings.get(jdbcUserSetting)));
        hikari.setPassword(ObjectUtils.to(String.class, settings.get(jdbcPasswordSetting)));
        LOGGER.info("Created data source: {}", hikari);

        return hikari;
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        return selectList(buildSelectStatement(query), query);
    }

    @Override
    public long readCount(Query<?> query) {
        String sqlQuery = buildCountStatement(query);

        return select(sqlQuery, query, result -> result.next()
                ? ObjectUtils.to(long.class, result.getObject(1))
                : 0L);
    }

    @Override
    public <T> T readFirst(Query<T> query) {
        return selectFirst(buildSelectStatement(query), query);
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        if (query.getSorters().isEmpty()) {
            if (!ObjectUtils.to(boolean.class, query.getOptions().get(DISABLE_BY_ID_ITERATOR_OPTION))) {
                return ByIdIterator.iterable(query, fetchSize);
            }
        }

        return selectIterable(buildSelectStatement(query), fetchSize, query);
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        String sqlQuery = buildLastUpdateStatement(query);

        return select(sqlQuery, query, result -> result.next()
                ? new Date((long) (ObjectUtils.to(double.class, result.getObject(1)) * 1000L))
                : null);
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {

        // Efficiently determine whether there are more items by:
        // 1. Guard against integer overflow in step 2.
        if (limit == Integer.MAX_VALUE) {
            -- limit;
        }

        // 2. Select one more item than requested.
        String sqlQuery = DSL.using(dialect())
                .selectFrom(DSL.table("(" + buildSelectStatement(query) + ")").as("q"))
                .offset((int) offset)
                .limit(limit + 1)
                .getSQL(ParamType.INLINED);

        List<T> items = selectList(sqlQuery, query);
        int size = items.size();

        // 3. If there are less items than the requested limit, there aren't
        // any more items after this result. For example, if there are 10 items
        // total matching the query, step 2 tries to fetch 11 items and would
        // trigger this.
        if (size <= limit) {
            return new PaginatedResult<>(offset, limit, offset + size, items);
        }

        // 4. Otherwise, there are more items, so remove the extra.
        items.remove(size - 1);

        // 5. And return a customized paginated result that calculates
        // the count on demand, as well as bypass the potentially expensive
        // #hasNext that uses #getCount.
        return new PaginatedResult<T>(offset, limit, 0, items) {

            private final Lazy<Long> count = new Lazy<Long>() {

                @Override
                protected Long create() {
                    return readCount(query);
                }
            };

            @Override
            public long getCount() {
                return count.get();
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        };
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        for (String field : fields) {
            Matcher groupingMatcher = Query.RANGE_PATTERN.matcher(field);
            if (groupingMatcher.find()) {
                throw new UnsupportedOperationException("SqlDatabase does not support group by numeric range");
            }
        }

        String sqlQuery = buildGroupStatement(query, fields);

        return select(sqlQuery, query, result -> {
            List<Grouping<T>> groupings = new ArrayList<>();
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
                    grouping = new SqlGrouping<>(keys, query, fields, count);
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
        });
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

            // Save all indexes.
            deleteIndexes(this, connection, context, null, indexStates);
            insertIndexes(this, connection, context, null, indexStates);

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
                        if (data == null) {
                            data = serializeState(state);
                        }

                        if (execute(connection, context, context
                                .insertInto(recordTable(),
                                        recordIdField(),
                                        recordTypeIdField(),
                                        recordDataField())
                                .select(context.select(
                                        DSL.inline(id, uuidType()),
                                        DSL.inline(typeId, uuidType()),
                                        DSL.inline(data, byteArrayType()))
                                        .whereNotExists(context
                                                .selectOne()
                                                .from(recordTable())
                                                .where(recordIdField().eq(id))
                                                .and(recordTypeIdField().eq(typeId))))) < 1) {

                            // INSERT failed so retry with UPDATE.
                            isNew = false;
                            continue;
                        }

                    } else {
                        List<AtomicOperation> atomicOperations = state.getAtomicOperations();

                        // Normal update.
                        if (atomicOperations.isEmpty()) {
                            if (data == null) {
                                data = serializeState(state);
                            }

                            if (execute(connection, context, context
                                    .update(recordTable())
                                    .set(recordTypeIdField(), typeId)
                                    .set(recordDataField(), data)
                                    .where(recordIdField().eq(id))) < 1) {

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
                                    .update(recordTable())
                                    .set(recordTypeIdField(), typeId)
                                    .set(recordDataField(), data)
                                    .where(recordIdField().eq(id))
                                    .and(recordTypeIdField().eq(oldTypeId))
                                    .and(recordDataField().eq(oldData))) < 1) {

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
                        if (execute(connection, context, context
                                .insertInto(recordUpdateTable(),
                                        recordUpdateIdField(),
                                        recordUpdateTypeIdField(),
                                        recordUpdateDateField())
                                .select(context.select(
                                        DSL.inline(id, uuidType()),
                                        DSL.inline(typeId, uuidType()),
                                        DSL.inline(now, doubleType()))
                                        .whereNotExists(context
                                                .selectOne()
                                                .from(recordUpdateTable())
                                                .where(recordUpdateIdField().eq(id))))) < 1) {

                            // INSERT failed so retry with UPDATE.
                            isNew = false;
                            continue;
                        }

                    } else {
                        if (execute(connection, context, context
                                .update(recordUpdateTable())
                                .set(recordUpdateTypeIdField(), typeId)
                                .set(recordUpdateDateField(), now)
                                .where(recordUpdateIdField().eq(id))) < 1) {

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
            deleteIndexes(this, connection, context, null, states);
            insertIndexes(this, connection, context, null, states);
        }
    }

    @Override
    public void doRecalculations(Connection connection, boolean isImmediate, ObjectIndex index, List<State> states) throws SQLException {
        try (DSLContext context = openContext(connection)) {
            deleteIndexes(this, connection, context, index, states);
            insertIndexes(this, connection, context, index, states);
        }
    }

    @Override
    protected void doDeletes(Connection connection, boolean isImmediate, List<State> states) throws SQLException {
        try (DSLContext context = openContext(connection)) {

            // Delete all indexes.
            deleteIndexes(this, connection, context, null, states);

            Set<UUID> stateIds = states.stream()
                    .map(State::getId)
                    .collect(Collectors.toSet());

            // Delete data.
            execute(connection, context, context
                    .delete(recordTable())
                    .where(recordIdField().in(stateIds)));

            // Save delete date.
            execute(connection, context, context
                    .update(recordUpdateTable())
                    .set(recordUpdateDateField(), System.currentTimeMillis() / 1000.0)
                    .where(recordUpdateIdField().in(stateIds)));
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

        public static byte[] getOriginalData(Object object) {
            return (byte[]) State.getInstance(object).getExtra(ORIGINAL_DATA_EXTRA);
        }
    }
}
