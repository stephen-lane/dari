package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.UUID;

abstract class AbstractSqlIndex {

    private final Table<Record> table;
    private final Field<UUID> id;
    private final Param<UUID> idParam;
    private final Field<UUID> typeId;
    private final Param<UUID> typeIdParam;
    private final Field<Integer> symbolId;
    private final Param<Integer> symbolIdParam;
    private final Field<Object> value;

    protected AbstractSqlIndex(SqlSchema schema, String namePrefix, int version) {
        DataType<Integer> integerType = schema.integerDataType();
        DataType<UUID> uuidType = schema.uuidDataType();

        this.table = DSL.table(DSL.name(namePrefix + version));
        this.id = DSL.field(DSL.name("id"), uuidType);
        this.idParam = DSL.param(id.getName(), uuidType);
        this.typeId = DSL.field(DSL.name("typeId"), uuidType);
        this.typeIdParam = DSL.param(typeId.getName(), uuidType);
        this.symbolId = DSL.field(DSL.name("symbolId"), integerType);
        this.symbolIdParam = DSL.param(symbolId.getName(), integerType);
        this.value = DSL.field(DSL.name("value"));
    }

    public Table<Record> table() {
        return table;
    }

    public Field<UUID> id() {
        return id;
    }

    public Param<UUID> idParam() {
        return idParam;
    }

    public Field<UUID> typeId() {
        return typeId;
    }

    public Param<UUID> typeIdParam() {
        return typeIdParam;
    }

    public Field<Integer> symbolId() {
        return symbolId;
    }

    public Param<Integer> symbolIdParam() {
        return symbolIdParam;
    }

    public Field<Object> value() {
        return value;
    }

    public abstract Object valueParam();

    public abstract Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value);
}
