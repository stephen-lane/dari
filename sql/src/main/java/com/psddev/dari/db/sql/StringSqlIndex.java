package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Locale;
import java.util.Map;

class StringSqlIndex extends AbstractSqlIndex {

    private final Param<String> valueParam;

    public StringSqlIndex(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);

        this.valueParam = DSL.param("value", schema.stringIndexType());
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> valueBindValues(ObjectIndex index, Object value) {
        String valueString = ObjectUtils.to(String.class, value);

        if (ObjectUtils.isBlank(valueString)) {
            return null;

        } else {
            valueString = StringUtils.trimAndCollapseWhitespaces(valueString);

            if (!index.isCaseSensitive()) {
                valueString = valueString.toLowerCase(Locale.ENGLISH);
            }

            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(valueParam.getName(), valueString);
            return bindValues;
        }
    }
}
