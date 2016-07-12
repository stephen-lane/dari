package com.psddev.dari.db.sql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import org.jooq.Field;

import java.util.Map;

class LocationSqlIndexTable extends SqlIndexTable {

    private Field<?> locationParam;

    public LocationSqlIndexTable(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.locationParam = schema.locationParam();
    }

    @Override
    public Field<?> valueParam() {
        return locationParam;
    }

    @Override
    public Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value) {
        value = convertValue(database, index, fieldIndex, value);

        if (!(value instanceof Location)) {
            return null;
        }

        Map<String, Object> bindValues = new CompactMap<>();

        schema.bindLocation(bindValues, (Location) value);

        return bindValues;
    }
}
