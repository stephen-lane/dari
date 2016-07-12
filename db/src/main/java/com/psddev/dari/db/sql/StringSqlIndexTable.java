package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.StringUtils;

import java.util.Locale;

class StringSqlIndexTable extends SqlIndexTable {

    public StringSqlIndexTable(SqlSchema schema, String namePrefix, int version) {
        super(schema, namePrefix, version);
    }

    @Override
    protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
        String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());

        if (!index.isCaseSensitive()) {
            valueString = valueString.toLowerCase(Locale.ENGLISH);
        }

        return stringToBytes(valueString, 500);
    }
}
