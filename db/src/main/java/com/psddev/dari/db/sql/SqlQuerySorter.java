package com.psddev.dari.db.sql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Sorter;
import org.jooq.Field;
import org.jooq.SortField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

import java.util.List;

@FunctionalInterface
interface SqlQuerySorter {

    SqlQuerySorter ASCENDING = (schema, join, options) -> join.valueField.sort(SortOrder.ASC);

    SqlQuerySorter DESCENDING = (schema, join, options) -> join.valueField.sort(SortOrder.DESC);

    SqlQuerySorter CLOSEST = (schema, join, options) -> distance(schema, join, options).sort(SortOrder.ASC);

    SqlQuerySorter FARTHEST = (schema, join, options) -> distance(schema, join, options).sort(SortOrder.DESC);

    static SqlQuerySorter find(String operator) {
        switch (operator) {
            case Sorter.ASCENDING_OPERATOR :
                return ASCENDING;

            case Sorter.DESCENDING_OPERATOR :
                return DESCENDING;

            case Sorter.CLOSEST_OPERATOR :
                return CLOSEST;

            case Sorter.FARTHEST_OPERATOR :
                return FARTHEST;

            default :
                return null;
        }
    }

    static Field<Double> distance(SqlSchema schema, SqlQueryJoin join, List<Object> options) {
        if (!(join.sqlIndex instanceof LocationSqlIndex)) {
            throw new IllegalArgumentException("Can't sort by distance against non-location field!");
        }

        return schema.stLength(
                schema.stMakeLine(
                        schema.stGeomFromText(DSL.inline(((Location) options.get(1)).toWkt())),
                        join.valueField));
    }

    SortField<?> createSortField(SqlSchema schema, SqlQueryJoin join, List<Object> options);
}
