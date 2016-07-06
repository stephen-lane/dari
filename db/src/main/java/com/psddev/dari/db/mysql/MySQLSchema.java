package com.psddev.dari.db.mysql;

import com.psddev.dari.db.sql.SqlSchema;
import com.psddev.dari.util.UuidUtils;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.util.mysql.MySQLDataType;

import java.util.UUID;

public class MySQLSchema extends SqlSchema {

    public static final MySQLSchema INSTANCE = new MySQLSchema();

    protected MySQLSchema() {
        super();
    }

    @Override
    protected DataType<UUID> uuid() {
        return MySQLDataType.BINARY.asConvertedDataType(new Converter<byte[], UUID>() {

            @Override
            public UUID from(byte[] bytes) {
                return UuidUtils.fromBytes(bytes);
            }

            @Override
            public byte[] to(UUID uuid) {
                return UuidUtils.toBytes(uuid);
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
    }
}
