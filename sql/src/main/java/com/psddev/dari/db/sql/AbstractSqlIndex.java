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

    protected final SqlSchema schema;
    private final Table<Record> table;
    private final Field<UUID> idField;
    private final Param<UUID> idParam;
    private final Field<UUID> typeIdField;
    private final Param<UUID> typeIdParam;
    private final Field<Integer> symbolIdField;
    private final Param<Integer> symbolIdParam;
    private final Field<Object> valueField;

    protected AbstractSqlIndex(SqlSchema schema, String namePrefix, int version) {
        DataType<Integer> integerType = schema.integerType();
        DataType<UUID> uuidType = schema.uuidType();

        this.schema = schema;
        this.table = DSL.table(DSL.name(namePrefix + version));
        this.idField = DSL.field(DSL.name("id"), uuidType);
        this.idParam = DSL.param(idField.getName(), uuidType);
        this.typeIdField = DSL.field(DSL.name("typeId"), uuidType);
        this.typeIdParam = DSL.param(typeIdField.getName(), uuidType);
        this.symbolIdField = DSL.field(DSL.name("symbolId"), integerType);
        this.symbolIdParam = DSL.param(symbolIdField.getName(), integerType);
        this.valueField = DSL.field(DSL.name("value"));
    }

    public Table<Record> table() {
        return table;
    }

    public Field<UUID> idField() {
        return idField;
    }

    public Param<UUID> idParam() {
        return idParam;
    }

    public Field<UUID> typeIdField() {
        return typeIdField;
    }

    public Param<UUID> typeIdParam() {
        return typeIdParam;
    }

    public Field<Integer> symbolIdField() {
        return symbolIdField;
    }

    public Param<Integer> symbolIdParam() {
        return symbolIdParam;
    }

    public Field<Object> valueField() {
        return valueField;
    }

    public abstract Object valueParam();

    public abstract Map<String, Object> valueBindValues(ObjectIndex index, Object value);

    public abstract Param<?> valueInline(ObjectIndex index, Object value);
}
