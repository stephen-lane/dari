package com.psddev.dari.db.mysql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import com.psddev.dari.db.sql.AbstractSqlDatabase;
import com.psddev.dari.db.sql.SqlSchema;
import com.psddev.dari.util.UuidUtils;
import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.util.mysql.MySQLDataType;

import java.util.Map;
import java.util.UUID;

public class MySQLSchema extends SqlSchema {

    private static final String LOCATION_PARAM_NAME = "location";
    private static final String REGION_PARAM_NAME = "region";

    protected MySQLSchema(MySQLDatabase database) {
        super(database);
    }

    @Override
    protected DataType<UUID> uuidDataType() {
        return MySQLDataType.BINARY.asConvertedDataType(new Converter<byte[], UUID>() {

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
    }

    @Override
    public Field<Object> locationParam() {
        return DSL.field("GeomFromText({0})", DSL.param(LOCATION_PARAM_NAME, String.class));
    }

    @Override
    public void bindLocation(Map<String, Object> bindValues, Location location) {
        bindValues.put(LOCATION_PARAM_NAME, "POINT(" + location.getX() + " " + location.getY() + ")");
    }

    @Override
    public Field<Object> regionParam() {
        return DSL.field("GeomFromText({0})", DSL.param(LOCATION_PARAM_NAME, String.class));
    }

    @Override
    public void bindRegion(Map<String, Object> bindValues, Region region) {
        StringBuilder mp = new StringBuilder();

        mp.append("MULTIPOLYGON(");

        for (Region.Polygon polygon : region.getPolygons()) {
            for (Region.LinearRing ring : polygon) {
                mp.append("((");
                for (Region.Coordinate coordinate : ring) {
                    mp.append(AbstractSqlDatabase.quoteValue(coordinate.getLatitude()));
                    mp.append(' ');
                    mp.append(AbstractSqlDatabase.quoteValue(coordinate.getLongitude()));
                    mp.append(", ");
                }
                mp.setLength(mp.length() - 2);
                mp.append(")), ");
            }
        }

        for (Region.Circle circles : region.getCircles()) {
            for (Region.Polygon polygon : circles.getPolygons()) {
                for (Region.LinearRing ring : polygon) {
                    mp.append("((");
                    for (Region.Coordinate coordinate : ring) {
                        mp.append(AbstractSqlDatabase.quoteValue(coordinate.getLatitude()));
                        mp.append(' ');
                        mp.append(AbstractSqlDatabase.quoteValue(coordinate.getLongitude()));
                        mp.append(", ");
                    }
                    mp.setLength(mp.length() - 2);
                    mp.append(")), ");
                }
            }
        }

        mp.setLength(mp.length() - 2);
        mp.append(")");

        bindValues.put(REGION_PARAM_NAME, mp.toString());
    }
}
