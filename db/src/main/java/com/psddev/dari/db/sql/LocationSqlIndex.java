package com.psddev.dari.db.sql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;

import java.util.Map;

class LocationSqlIndex extends AbstractSqlIndex {

    private final Object locationParam;

    public LocationSqlIndex(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.locationParam = schema.locationParam();
    }

    @Override
    public Object valueParam() {
        return locationParam;
    }

    @Override
    public Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value) {
        if (value instanceof Location) {
            Map<String, Object> bindValues = new CompactMap<>();
            schema.bindLocation(bindValues, (Location) value);
            return bindValues;

        } else {
            return null;
        }
    }
}
