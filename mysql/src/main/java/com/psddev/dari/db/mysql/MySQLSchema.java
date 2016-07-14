package com.psddev.dari.db.mysql;

import com.psddev.dari.db.sql.SqlSchema;
import com.psddev.dari.util.UuidUtils;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.util.mysql.MySQLDataType;

import java.util.UUID;

public class MySQLSchema extends SqlSchema {

    private static final DataType<UUID> UUID_TYPE = MySQLDataType.BINARY.asConvertedDataType(new Converter<byte[], UUID>() {

        @Override
        public UUID from(byte[] bytes) {
            return bytes != null ? UuidUtils.fromBytes(bytes) : null;
        }

        @Override
        public byte[] to(UUID uuid) {
            return uuid != null ? UuidUtils.toBytes(uuid) : null;
        }

        @Override
        public Class<byte[]> fromType() {
            return byte[].class;
        }

        @Override
        public Class<UUID> toType() {
            return UUID.class;
        }
    });

    protected MySQLSchema(MySQLDatabase database) {
        super(database);
    }

    @Override
    public DataType<UUID> uuidType() {
        return UUID_TYPE;
    }

    @Override
    public Condition stContains(Field<Object> x, Field<Object> y) {
        return DSL.condition("MBRContains({0}, {1})", x, y);
    }

    @Override
    public Field<Object> stGeomFromText(Field<String> wkt) {
        return DSL.field("GeomFromText({0})", wkt);
    }

    @Override
    public Field<Double> stLength(Field<Object> field) {
        return DSL.field("GLength({0})", Double.class, field);
    }

    @Override
    public Field<Object> stMakeLine(Field<Object> x, Field<Object> y) {
        return DSL.field("LineString({0}, {1})", x, y);
    }
}
