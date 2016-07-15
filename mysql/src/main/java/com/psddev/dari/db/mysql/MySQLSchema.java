package com.psddev.dari.db.mysql;

import com.psddev.dari.db.sql.SqlSchema;
import com.psddev.dari.util.UuidUtils;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.util.mysql.MySQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class MySQLSchema extends SqlSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSchema.class);

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

    private volatile Boolean binlogFormatStatement;

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

    @Override
    public void setTransactionIsolation(Connection connection) throws SQLException {
        if (binlogFormatStatement == null) {
            synchronized (this) {
                if (binlogFormatStatement == null) {
                    try (Statement statement = connection.createStatement();
                            ResultSet result = statement.executeQuery("SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format')")) {

                        boolean logBin = false;

                        while (result.next()) {
                            String name = result.getString(1);
                            String value = result.getString(2);

                            if ("binlog_format".equalsIgnoreCase(name)) {
                                binlogFormatStatement = "STATEMENT".equalsIgnoreCase(value);

                            } else if ("log_bin".equalsIgnoreCase(name)) {
                                logBin = !"OFF".equalsIgnoreCase(value);
                            }
                        }

                        binlogFormatStatement = logBin && Boolean.TRUE.equals(binlogFormatStatement);

                        if (binlogFormatStatement) {
                            LOGGER.warn("Can't set transaction isolation to"
                                    + " READ COMMITTED because binlog_format"
                                    + " is set to STATEMENT. Please set it to"
                                    + " MIXED (my.cnf: binlog_format = mixed)"
                                    + " to prevent reduced performance under"
                                    + " load.");
                        }
                    }
                }
            }
        }

        if (!binlogFormatStatement) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }
}
