package com.psddev.dari.db.sql;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Region;
import com.psddev.dari.db.State;
import org.jooq.BatchBindStep;
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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SqlSchema {

    private static final DataType<String> BYTE_STRING_TYPE = SQLDataType.LONGVARBINARY.asConvertedDataType(new Converter<byte[], String>() {

        @Override
        public String from(byte[] bytes) {
            return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
        }

        @Override
        public byte[] to(String string) {
            return string != null ? string.getBytes(StandardCharsets.UTF_8) : null;
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

    private final Table<Record> record;
    private final Field<UUID> recordId;
    private final Field<UUID> recordTypeId;
    private final Field<byte[]> recordData;

    private final Table<Record> recordUpdate;
    private final Field<UUID> recordUpdateId;
    private final Field<UUID> recordUpdateTypeId;
    private final Field<Double> recordUpdateDate;

    private final Table<Record> symbol;
    private final Field<Integer> symbolId;
    private final Field<String> symbolValue;

    private final List<AbstractSqlIndex> locationTables;
    private final List<AbstractSqlIndex> numberTables;
    private final List<AbstractSqlIndex> regionTables;
    private final List<AbstractSqlIndex> stringTables;
    private final List<AbstractSqlIndex> uuidTables;
    private final List<AbstractSqlIndex> deleteTables;

    protected SqlSchema(AbstractSqlDatabase database) {
        DataType<byte[]> byteArrayDataType = byteArrayDataType();
        DataType<String> byteStringDataType = byteStringDataType();
        DataType<Double> doubleDataType = doubleDataType();
        DataType<Integer> integerDataType = integerDataType();
        DataType<UUID> uuidDataType = uuidDataType();

        record = DSL.table(DSL.name("Record"));
        recordId = DSL.field(DSL.name("id"), uuidDataType);
        recordTypeId = DSL.field(DSL.name("typeId"), uuidDataType);
        recordData = DSL.field(DSL.name("data"), byteArrayDataType);

        recordUpdate = DSL.table(DSL.name("RecordUpdate"));
        recordUpdateId = DSL.field(DSL.name("id"), uuidDataType);
        recordUpdateTypeId = DSL.field(DSL.name("typeId"), uuidDataType);
        recordUpdateDate = DSL.field(DSL.name("updateDate"), doubleDataType);

        symbol = DSL.table(DSL.name("Symbol"));
        symbolId = DSL.field(DSL.name("symbolId"), integerDataType);
        symbolValue = DSL.field(DSL.name("value"), byteStringDataType);

        numberTables = supportedTables(database, new NumberSqlIndex(this, "RecordNumber", 3));
        stringTables = supportedTables(database, new StringSqlIndex(this, "RecordString", 4));
        uuidTables = supportedTables(database, new UuidSqlIndex(this, "RecordUuid", 3));

        if (database.isIndexSpatial()) {
            locationTables = possiblyUnsupportedTables(
                    database,
                    () -> new LocationSqlIndex(this, "RecordLocation", 3));

            regionTables = possiblyUnsupportedTables(
                    database,
                    () -> new RegionSqlIndex(this, "RecordRegion", 2));

        } else {
            locationTables = Collections.emptyList();
            regionTables = Collections.emptyList();
        }

        deleteTables = ImmutableList.<AbstractSqlIndex>builder()
                .addAll(locationTables)
                .addAll(numberTables)
                .addAll(regionTables)
                .addAll(stringTables)
                .addAll(uuidTables)
                .build();
    }

    private List<AbstractSqlIndex> supportedTables(AbstractSqlDatabase database, AbstractSqlIndex... tables) {
        ImmutableList.Builder<AbstractSqlIndex> builder = ImmutableList.builder();
        boolean empty = true;

        for (AbstractSqlIndex table : tables) {
            if (database.hasTable(table.table().getName())) {
                builder.add(table);
                empty = false;
            }
        }

        if (empty) {
            int length = tables.length;

            if (length > 0) {
                builder.add(tables[length - 1]);
            }
        }

        return builder.build();
    }

    @SafeVarargs
    private final List<AbstractSqlIndex> possiblyUnsupportedTables(AbstractSqlDatabase database, Supplier<AbstractSqlIndex>... suppliers) {
        ImmutableList.Builder<AbstractSqlIndex> builder = ImmutableList.builder();

        for (Supplier<AbstractSqlIndex> supplier : suppliers) {
            AbstractSqlIndex table;

            try {
                table = supplier.get();

            } catch (UnsupportedOperationException error) {
                continue;
            }

            if (database.hasTable(table.table().getName())) {
                builder.add(table);
            }
        }

        return builder.build();
    }

    protected DataType<byte[]> byteArrayDataType() {
        return SQLDataType.LONGVARBINARY;
    }

    protected DataType<String> byteStringDataType() {
        return BYTE_STRING_TYPE;
    }

    protected DataType<Double> doubleDataType() {
        return SQLDataType.DOUBLE;
    }

    protected DataType<Integer> integerDataType() {
        return SQLDataType.INTEGER;
    }

    protected DataType<UUID> uuidDataType() {
        return SQLDataType.UUID;
    }

    public Object locationParam() {
        throw new UnsupportedOperationException();
    }

    public void bindLocation(Map<String, Object> bindValues, Location location) {
        throw new UnsupportedOperationException();
    }

    public Object regionParam() {
        throw new UnsupportedOperationException();
    }

    public void bindRegion(Map<String, Object> bindValues, Region region) {
        throw new UnsupportedOperationException();
    }

    public Table<Record> record() {
        return record;
    }

    public Field<UUID> recordId() {
        return recordId;
    }

    public Field<UUID> recordTypeId() {
        return recordTypeId;
    }

    public Field<byte[]> recordData() {
        return recordData;
    }

    public Table<Record> recordUpdate() {
        return recordUpdate;
    }

    public Field<UUID> recordUpdateId() {
        return recordUpdateId;
    }

    public Field<UUID> recordUpdateTypeId() {
        return recordUpdateTypeId;
    }

    public Field<Double> recordUpdateDate() {
        return recordUpdateDate;
    }

    public Table<Record> symbol() {
        return symbol;
    }

    public Field<Integer> symbolId() {
        return symbolId;
    }

    public Field<String> symbolValue() {
        return symbolValue;
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
                return uuidTables;

            case ObjectField.DATE_TYPE :
            case ObjectField.NUMBER_TYPE :
                return numberTables;

            case ObjectField.LOCATION_TYPE :
                return locationTables;

            case ObjectField.REGION_TYPE :
                return regionTables;

            default :
                return stringTables;
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

        for (State state : states) {
            UUID id = state.getId();
            UUID typeId = state.getVisibilityAwareTypeId();

            for (SqlIndexValue indexValue : SqlIndex.getIndexValues(state)) {
                ObjectIndex index = indexValue.getIndex();

                if (onlyIndex != null && !onlyIndex.equals(index)) {
                    continue;
                }

                for (AbstractSqlIndex table : findUpdateIndexTables(index)) {
                    Table<Record> jooqTable = table.table();
                    BatchBindStep batch = batches.get(jooqTable);
                    Param<UUID> idParam = table.idParam();
                    Param<UUID> typeIdParam = table.typeIdParam();
                    Param<Integer> symbolIdParam = table.symbolIdParam();

                    if (batch == null) {
                        batch = context.batch(context.insertInto(jooqTable)
                                .set(table.id(), idParam)
                                .set(table.typeId(), typeIdParam)
                                .set(table.symbolId(), symbolIdParam)
                                .set(table.value(), table.valueParam())
                                .onDuplicateKeyIgnore());
                    }

                    Object key = table.convertKey(database, index, indexValue.getUniqueName());
                    boolean bound = false;

                    for (Object[] valuesArray : indexValue.getValuesArray()) {
                        Map<String, Object> bindValues = table.createBindValues(database, this, index, 0, valuesArray[0]);

                        if (bindValues != null) {
                            bindValues.put(idParam.getName(), id);
                            bindValues.put(typeIdParam.getName(), typeId);
                            bindValues.put(symbolIdParam.getName(), key);

                            batch = batch.bind(bindValues);
                            bound = true;
                        }
                    }

                    if (bound) {
                        batches.put(jooqTable, batch);
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

        for (AbstractSqlIndex table : deleteTables) {
            try {
                DeleteConditionStep<Record> delete = context
                        .deleteFrom(table.table())
                        .where(table.id().in(stateIds));

                if (onlyIndex != null) {
                    delete = delete.and(table.symbolId().eq(database.getReadSymbolId(onlyIndex.getUniqueName())));
                }

                context.execute(delete);

            } catch (DataAccessException error) {
                Throwables.propagateIfInstanceOf(error, SQLException.class);
                throw error;
            }
        }
    }
}
