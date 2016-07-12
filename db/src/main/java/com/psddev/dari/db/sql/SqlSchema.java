package com.psddev.dari.db.sql;

import com.google.common.collect.ImmutableList;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Region;
import com.psddev.dari.util.StringUtils;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class SqlSchema {

    public static final SqlSchema INSTANCE = new SqlSchema();

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

    private final List<SqlIndexTable> locationTables;
    private final List<SqlIndexTable> numberTables;
    private final List<SqlIndexTable> regionTables;
    private final List<SqlIndexTable> stringTables;
    private final List<SqlIndexTable> uuidTables;

    protected SqlSchema() {
        DataType<byte[]> byteArrayDataType = byteArrayDataType();
        DataType<Double> doubleDataType = doubleDataType();
        DataType<Integer> integerDataType = integerDataType();
        DataType<String> stringDataType = stringDataType();
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
        symbolValue = DSL.field(DSL.name("value"), stringDataType);

        locationTables = ImmutableList.of(new LocationSqlIndexTable(this, "RecordLocation", 3));
        numberTables = ImmutableList.of(new SqlIndexTable(this, "RecordNumber", 3));
        regionTables = ImmutableList.of(new RegionSqlIndexTable(this, "RecordRegion", 2));

        stringTables = ImmutableList.of(
                new SqlIndexTable(this, "RecordString", 4) {

                    @Override
                    protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                        String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());

                        if (!index.isCaseSensitive()) {
                            valueString = valueString.toLowerCase(Locale.ENGLISH);
                        }

                        return stringToBytes(valueString, 500);
                    }
                });

        uuidTables = ImmutableList.of(new SqlIndexTable(this, "RecordUuid", 3));
    }

    protected DataType<byte[]> byteArrayDataType() {
        return SQLDataType.LONGVARBINARY;
    }

    protected DataType<Double> doubleDataType() {
        return SQLDataType.DOUBLE;
    }

    protected DataType<Integer> integerDataType() {
        return SQLDataType.INTEGER;
    }

    protected DataType<String> stringDataType() {
        return SQLDataType.LONGVARBINARY.asConvertedDataType(new Converter<byte[], String>() {

            @Override
            public String from(byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }

            @Override
            public byte[] to(String string) {
                return string.getBytes(StandardCharsets.UTF_8);
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
    }

    protected DataType<UUID> uuidDataType() {
        return SQLDataType.UUID;
    }

    public Field<Object> locationParam() {
        throw new UnsupportedOperationException();
    }

    public void bindLocation(Map<String, Object> bindValues, Location location) {
        throw new UnsupportedOperationException();
    }

    public Field<Object> regionParam() {
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

    private List<SqlIndexTable> findIndexTables(String type) {
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

    /**
     * Finds the index table that should be used with SELECT SQL queries.
     *
     * @param database Can't be {@code null}.
     * @param type May be {@code null}.
     * @return Never {@code null}.
     */
    public SqlIndexTable findSelectIndexTable(AbstractSqlDatabase database, String type) {
        List<SqlIndexTable> tables = findIndexTables(type);

        for (SqlIndexTable table : tables) {
            if (database.hasTable(table.getName())) {
                return table;
            }
        }

        return tables.get(tables.size() - 1);
    }

    private String indexType(ObjectIndex index) {
        List<String> fieldNames = index.getFields();
        ObjectField field = index.getParent().getField(fieldNames.get(0));

        return field != null ? field.getInternalItemType() : index.getType();
    }

    public SqlIndexTable findSelectIndexTable(AbstractSqlDatabase database, ObjectIndex index) {
        return findSelectIndexTable(database, indexType(index));
    }

    /**
     * Finds all the index tables that should be used with UPDATE SQL queries.
     *
     * @param database Can't be {@code null}.
     * @param type May be {@code null}.
     * @return Never {@code null}.
     */
    public List<SqlIndexTable> findUpdateIndexTables(AbstractSqlDatabase database, String type) {
        if (!database.isIndexSpatial()
                && (ObjectField.LOCATION_TYPE.equals(type)
                || ObjectField.REGION_TYPE.equals(type))) {

            return Collections.emptyList();
        }

        List<SqlIndexTable> tables = findIndexTables(type);
        List<SqlIndexTable> writeTables = tables.stream()
                .filter(t -> database.hasTable(t.getName()))
                .collect(Collectors.toList());

        if (writeTables.isEmpty()) {
            writeTables.add(tables.get(tables.size() - 1));
        }

        return writeTables;
    }

    public List<SqlIndexTable> findUpdateIndexTables(AbstractSqlDatabase database, ObjectIndex index) {
        return findUpdateIndexTables(database, indexType(index));
    }

    /**
     * Finds all the index tables that should be used with DELETE SQL queries.
     *
     * @param database Can't be {@code null}.
     * @return Never {@code null}.
     */
    public List<SqlIndexTable> findDeleteIndexTables(AbstractSqlDatabase database) {
        List<SqlIndexTable> deleteTables = new ArrayList<>();

        if (database.isIndexSpatial()) {
            deleteTables.addAll(locationTables);
            deleteTables.addAll(regionTables);
        }

        deleteTables.addAll(numberTables);
        deleteTables.addAll(stringTables);
        deleteTables.addAll(uuidTables);

        deleteTables.removeIf(t -> !database.hasTable(t.getName()));

        return deleteTables;
    }
}
