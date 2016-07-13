package com.psddev.dari.db.sql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;

class LocationSqlIndex extends AbstractSqlIndex {

    private final Param<String> locationParam;
    private final Object valueParam;

    public LocationSqlIndex(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.locationParam = DSL.param("location", String.class);
        this.valueParam = schema.stGeomFromText(locationParam);
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value) {
        if (value instanceof Location) {
            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(locationParam.getName(), ((Location) value).toWkt());
            return bindValues;

        } else {
            return null;
        }
    }
}
