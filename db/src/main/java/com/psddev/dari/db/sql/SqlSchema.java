package com.psddev.dari.db.sql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

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
}
