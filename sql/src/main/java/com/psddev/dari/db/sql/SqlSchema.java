package com.psddev.dari.db.sql;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.State;
import com.psddev.dari.util.IoUtils;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.DeleteConditionStep;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SqlSchema {

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

    private final List<AbstractSqlIndex> locationSqlIndexes;
    private final List<AbstractSqlIndex> numberSqlIndexes;
    private final List<AbstractSqlIndex> regionSqlIndexes;
    private final List<AbstractSqlIndex> stringSqlIndexes;
    private final List<AbstractSqlIndex> uuidSqlIndexes;
    private final List<AbstractSqlIndex> deleteSqlIndexes;

    protected SqlSchema(AbstractSqlDatabase database) {
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

        numberSqlIndexes = supportedTables(database, new NumberSqlIndex(this, "RecordNumber", 3));
        stringSqlIndexes = supportedTables(database, new StringSqlIndex(this, "RecordString", 4));
        uuidSqlIndexes = supportedTables(database, new UuidSqlIndex(this, "RecordUuid", 3));

        if (database.isIndexSpatial()) {
            locationSqlIndexes = possiblyUnsupportedTables(
                    database,
                    () -> new LocationSqlIndex(this, "RecordLocation", 3));

            regionSqlIndexes = possiblyUnsupportedTables(
                    database,
                    () -> new RegionSqlIndex(this, "RecordRegion", 2));

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

    private List<AbstractSqlIndex> supportedTables(AbstractSqlDatabase database, AbstractSqlIndex... sqlIndexes) {
        ImmutableList.Builder<AbstractSqlIndex> builder = ImmutableList.builder();
        boolean empty = true;

        for (AbstractSqlIndex sqlIndex : sqlIndexes) {
            if (database.hasTable(sqlIndex.table().getName())) {
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

    @SafeVarargs
    private final List<AbstractSqlIndex> possiblyUnsupportedTables(AbstractSqlDatabase database, Supplier<AbstractSqlIndex>... suppliers) {
        ImmutableList.Builder<AbstractSqlIndex> builder = ImmutableList.builder();

        for (Supplier<AbstractSqlIndex> supplier : suppliers) {
            AbstractSqlIndex sqlIndex;

            try {
                sqlIndex = supplier.get();

            } catch (UnsupportedOperationException error) {
                continue;
            }

            if (database.hasTable(sqlIndex.table().getName())) {
                builder.add(sqlIndex);
            }
        }

        return builder.build();
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
}
