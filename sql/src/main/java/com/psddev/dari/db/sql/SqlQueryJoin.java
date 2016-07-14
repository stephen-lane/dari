package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.SqlDatabase;
import com.psddev.dari.db.UnsupportedIndexException;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class SqlQueryJoin {

    public Predicate parent;
    private boolean leftOuter;

    private final SqlQuery sqlQuery;
    private final String queryKey;
    private final ObjectIndex index;

    public final boolean needsIndexTable;
    public final boolean needsIsNotNull;

    public final AbstractSqlIndex sqlIndex;
    public final Table<?> table;
    public final Field<Object> idField;
    public final Field<Object> typeIdField;
    public final Field<Object> symbolIdField;
    public final Field<Object> valueField;

    public final Set<Integer> symbolIds = new HashSet<>();

    public static SqlQueryJoin create(SqlQuery sqlQuery, String queryKey) {
        List<SqlQueryJoin> joins = sqlQuery.joins;
        String alias = "i" + joins.size();
        SqlQueryJoin join = new SqlQueryJoin(sqlQuery, alias, queryKey);

        joins.add(join);

        if (queryKey.equals(sqlQuery.query.getOptions().get(SqlDatabase.MYSQL_INDEX_HINT_QUERY_OPTION))) {
            sqlQuery.mysqlIndexHint = join;
        }

        return join;
    }

    public static SqlQueryJoin findOrCreate(SqlQuery sqlQuery, String queryKey) {
        Map<String, ObjectIndex> selectedIndexes = sqlQuery.selectedIndexes;
        ObjectIndex index = selectedIndexes.get(queryKey);

        for (SqlQueryJoin join : sqlQuery.joins) {
            if (queryKey.equals(join.queryKey)) {
                return join;

            } else {
                Map<String, Query.MappedKey> mappedKeys = sqlQuery.mappedKeys;
                String indexKey = sqlQuery.mappedKeys.get(queryKey).getIndexKey(index);

                if (indexKey != null
                        && indexKey.equals(mappedKeys.get(join.queryKey).getIndexKey(join.index))) {

                    return join;
                }
            }
        }

        return create(sqlQuery, queryKey);
    }

    @SuppressWarnings("unchecked")
    private SqlQueryJoin(SqlQuery sqlQuery, String alias, String queryKey) {
        this.sqlQuery = sqlQuery;
        this.queryKey = queryKey;
        this.index = sqlQuery.selectedIndexes.get(queryKey);

        switch (queryKey) {
            case Query.ID_KEY :
                needsIndexTable = false;
                needsIsNotNull = true;

                sqlIndex = null;
                table = null;
                idField = null;
                typeIdField = null;
                symbolIdField = null;
                valueField = (Field) sqlQuery.recordIdField;

                break;

            case Query.TYPE_KEY :
                needsIndexTable = false;
                needsIsNotNull = true;

                sqlIndex = null;
                table = null;
                idField = null;
                typeIdField = null;
                symbolIdField = null;
                valueField = (Field) sqlQuery.recordTypeIdField;

                break;

            case Query.COUNT_KEY :
                needsIndexTable = false;
                needsIsNotNull = false;

                sqlIndex = null;
                table = null;
                idField = null;
                typeIdField = null;
                symbolIdField = null;
                valueField = (Field) sqlQuery.recordIdField.count();

                break;

            case Query.ANY_KEY :
            case Query.LABEL_KEY :
                throw new UnsupportedIndexException(sqlQuery.database, queryKey);

            default :
                needsIndexTable = true;
                needsIsNotNull = true;

                sqlIndex = sqlQuery.schema.findSelectIndexTable(index);
                table = DSL.table(DSL.name(sqlIndex.table().getName())).as(sqlQuery.aliasPrefix + alias);
                idField = sqlQuery.aliasedField(alias, sqlIndex.idField().getName());
                typeIdField = sqlQuery.aliasedField(alias, sqlIndex.typeIdField().getName());
                symbolIdField = sqlQuery.aliasedField(alias, sqlIndex.symbolIdField().getName());
                valueField = sqlQuery.aliasedField(alias, sqlIndex.valueField().getName());

                addSymbolId(queryKey);
                break;
        }
    }

    public boolean isLeftOuter() {
        return leftOuter;
    }

    public void useLeftOuter() {
        leftOuter = true;
    }

    public void addSymbolId(String queryKey) {
        String indexKey = sqlQuery.mappedKeys.get(queryKey).getIndexKey(sqlQuery.selectedIndexes.get(queryKey));

        if (ObjectUtils.isBlank(indexKey)) {
            throw new UnsupportedIndexException(sqlQuery.database, indexKey);
        }

        if (needsIndexTable) {
            symbolIds.add(sqlQuery.database.getSymbolId(indexKey));
        }
    }

    public Object value(Object value) {
        if (sqlIndex == null) {
            switch (queryKey) {
                case Query.ID_KEY :
                case Query.TYPE_KEY :
                    return DSL.inline(ObjectUtils.to(UUID.class, value), sqlQuery.schema.uuidType());

                default :
                    return value;
            }

        } else if (sqlIndex instanceof NumberSqlIndex) {
            return DSL.inline(ObjectUtils.to(double.class, value), sqlQuery.schema.doubleType());

        } else if (sqlIndex instanceof StringSqlIndex) {
            String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());

            if (!index.isCaseSensitive()) {
                valueString = valueString.toLowerCase(Locale.ENGLISH);
            }

            return DSL.inline(valueString, sqlQuery.schema.byteStringType());

        } else if (sqlIndex instanceof UuidSqlIndex) {
            return DSL.inline(ObjectUtils.to(UUID.class, value), sqlQuery.schema.uuidType());

        } else {
            return value;
        }
    }
}
