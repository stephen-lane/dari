package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Region;
import com.psddev.dari.util.CompactMap;

import java.util.Map;

class RegionSqlIndex extends AbstractSqlIndex {

    private final Object regionParam;

    public RegionSqlIndex(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.regionParam = schema.regionParam();
    }

    @Override
    public Object valueParam() {
        return regionParam;
    }

    @Override
    public Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value) {
        if (value instanceof Region) {
            Map<String, Object> bindValues = new CompactMap<>();
            schema.bindRegion(bindValues, (Region) value);
            return bindValues;

        } else {
            return null;
        }
    }
}
