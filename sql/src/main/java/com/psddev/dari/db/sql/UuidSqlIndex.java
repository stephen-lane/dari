package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.UUID;

class UuidSqlIndex extends AbstractSqlIndex {

    private final Param<UUID> valueParam;

    public UuidSqlIndex(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.valueParam = DSL.param("value", schema.uuidType());
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> valueBindValues(ObjectIndex index, Object value) {
        UUID valueUuid = ObjectUtils.to(UUID.class, value);

        if (valueUuid == null) {
            return null;

        } else {
            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(valueParam.getName(), valueUuid);
            return bindValues;
        }
    }

    @Override
    public Param<?> valueInline(ObjectIndex index, Object value) {
        return DSL.inline(ObjectUtils.to(UUID.class, value), schema.uuidType());
    }
}
