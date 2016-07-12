package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Region;
import com.psddev.dari.util.CompactMap;
import org.jooq.Field;

import java.util.Map;

class RegionSqlIndexTable extends SqlIndexTable {

    private final Field<?> regionParam;

    public RegionSqlIndexTable(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.regionParam = schema.regionParam();
    }

    @Override
    public Field<?> valueParam() {
        return regionParam;
    }

    @Override
    public Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value) {
        value = convertValue(database, index, fieldIndex, value);

        if (!(value instanceof Region)) {
            return null;
        }

        Map<String, Object> bindValues = new CompactMap<>();

        schema.bindRegion(bindValues, (Region) value);

        return bindValues;
    }
}
