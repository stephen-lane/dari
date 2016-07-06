package com.psddev.dari.db.sql;

import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SqlSchema {

    public static final SqlSchema INSTANCE = new SqlSchema();

    private final Table<Record> record;
    private final Field<UUID> recordId;
    private final Field<UUID> recordTypeId;
    private final Field<String> recordData;

    private final Table<Record> symbol;
    private final Field<Integer> symbolId;
    private final Field<String> symbolValue;

    protected SqlSchema() {
        DataType<Integer> integer = integer();
        DataType<String> string = string();
        DataType<UUID> uuid = uuid();

        record = DSL.table(DSL.name("Record"));
        recordId = DSL.field(DSL.name("id"), uuid);
        recordTypeId = DSL.field(DSL.name("typeId"), uuid);
        recordData = DSL.field(DSL.name("data"), string);

        symbol = DSL.table(DSL.name("Symbol"));
        symbolId = DSL.field(DSL.name("symbolId"), integer);
        symbolValue = DSL.field(DSL.name("value"), string);
    }

    protected DataType<Integer> integer() {
        return SQLDataType.INTEGER;
    }

    protected DataType<String> string() {
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

    protected DataType<UUID> uuid() {
        return SQLDataType.UUID;
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

    public Field<String> recordData() {
        return recordData;
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
