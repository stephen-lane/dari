package com.psddev.dari.db;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

class SqlQueryJoin {

    public Predicate parent;
    public JoinType type = JoinType.JOIN;

    public final boolean needsIndexTable;
    public final boolean needsIsNotNull;
    public final String queryKey;
    public final String indexType;
    public final Table<?> table;
    public final Field<Object> idField;
    public final Field<Object> typeIdField;
    public final Field<Object> keyField;
    public final List<String> indexKeys = new ArrayList<>();

    private final SqlQuery sqlQuery;
    private final String alias;
    public final ObjectIndex index;
    public final SqlIndex sqlIndex;
    public final SqlIndex.Table sqlIndexTable;
    private final Field<?> valueField;
    public final String hashAttribute;

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
                        && indexKey.equals(mappedKeys.get(join.queryKey).getIndexKey(join.index))
                        && ((mappedKeys.get(queryKey).getHashAttribute() != null && mappedKeys.get(queryKey).getHashAttribute().equals(join.hashAttribute))
                        || (mappedKeys.get(queryKey).getHashAttribute() == null && join.hashAttribute == null))) {

                    return join;
                }
            }
        }

        return create(sqlQuery, queryKey);
    }

    public static SqlQueryJoin findOrCreateForSort(SqlQuery sqlQuery, String queryKey) {
        SqlQueryJoin join = findOrCreate(sqlQuery, queryKey);

        join.type = JoinType.LEFT_OUTER_JOIN;
        return join;
    }

    public SqlQueryJoin(SqlQuery sqlQuery, String alias, String queryKey) {
        this.sqlQuery = sqlQuery;
        this.alias = alias;
        this.queryKey = queryKey;

        Query.MappedKey mappedKey = sqlQuery.mappedKeys.get(queryKey);
        this.hashAttribute = mappedKey.getHashAttribute();
        this.index = sqlQuery.selectedIndexes.get(queryKey);

        this.indexType = mappedKey.getInternalType();
        this.sqlIndex = this.index != null
                ? SqlIndex.Static.getByIndex(this.index)
                : SqlIndex.Static.getByType(this.indexType);

        switch (queryKey) {
            case Query.ID_KEY :
                needsIndexTable = false;
                valueField = sqlQuery.recordIdField;
                sqlIndexTable = null;
                table = null;
                idField = null;
                typeIdField = null;
                keyField = null;
                needsIsNotNull = true;
                break;

            case Query.TYPE_KEY :
                needsIndexTable = false;
                valueField = sqlQuery.recordTypeIdField;
                sqlIndexTable = null;
                table = null;
                idField = null;
                typeIdField = null;
                keyField = null;
                needsIsNotNull = true;
                break;

            case Query.COUNT_KEY :
                needsIndexTable = false;
                valueField = sqlQuery.recordIdField.count();
                sqlIndexTable = null;
                table = null;
                idField = null;
                typeIdField = null;
                keyField = null;
                needsIsNotNull = false;
                break;

            case Query.ANY_KEY :
            case Query.LABEL_KEY :
                throw new UnsupportedIndexException(sqlQuery.database, queryKey);

            default :
                needsIndexTable = true;
                addIndexKey(queryKey);
                valueField = null;
                sqlIndexTable = this.sqlIndex.getReadTable(sqlQuery.database, index);
                table = DSL.table(DSL.name(sqlIndexTable.getName(sqlQuery.database, index))).as(sqlQuery.aliasPrefix + alias);
                idField = sqlQuery.aliasedField(alias, sqlIndexTable.getIdField(sqlQuery.database, index));
                typeIdField = sqlQuery.aliasedField(alias, sqlIndexTable.getTypeIdField(sqlQuery.database, index));
                keyField = sqlQuery.aliasedField(alias, sqlIndexTable.getKeyField(sqlQuery.database, index));
                needsIsNotNull = true;
                break;
        }
    }

    public void addIndexKey(String queryKey) {
        String indexKey = sqlQuery.mappedKeys.get(queryKey).getIndexKey(sqlQuery.selectedIndexes.get(queryKey));

        if (ObjectUtils.isBlank(indexKey)) {
            throw new UnsupportedIndexException(sqlQuery.database, indexKey);
        }

        if (needsIndexTable) {
            indexKeys.add(indexKey);
        }
    }

    public Object convertIndexKey(String indexKey) {
        return sqlIndexTable.convertKey(sqlQuery.database, index, indexKey);
    }

    public Object convertValue(ComparisonPredicate comparison, Object value) {
        Query.MappedKey mappedKey = sqlQuery.mappedKeys.get(comparison.getKey());
        ObjectField field = mappedKey.getField();
        SqlIndex fieldSqlIndex = field != null
                ? SqlIndex.Static.getByType(field.getInternalItemType())
                : sqlIndex;

        if (fieldSqlIndex == SqlIndex.UUID) {
            value = ObjectUtils.to(UUID.class, value);

        } else if (fieldSqlIndex == SqlIndex.NUMBER
                && !PredicateParser.STARTS_WITH_OPERATOR.equals(comparison.getOperator())) {

            if (value != null) {
                Long valueLong = ObjectUtils.to(Long.class, value);

                if (valueLong != null) {
                    value = valueLong;

                } else {
                    value = ObjectUtils.to(Double.class, value);
                }
            }

        } else if (fieldSqlIndex == SqlIndex.STRING) {
            if (comparison.isIgnoreCase()) {
                value = value.toString().toLowerCase(Locale.ENGLISH);

            } else if (sqlQuery.database.comparesIgnoreCase()) {
                String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());

                if (!index.isCaseSensitive()) {
                    valueString = valueString.toLowerCase(Locale.ENGLISH);
                }

                value = valueString;
            }
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    public Field<Object> getValueField(String queryKey, ComparisonPredicate comparison) {
        Field<?> field;

        if (valueField != null) {
            field = valueField;

        } else if (sqlIndex != SqlIndex.CUSTOM) {
            field = sqlQuery.aliasedField(alias, sqlIndexTable.getValueField(sqlQuery.database, index, 0));

        } else {
            String valueFieldName = sqlQuery.mappedKeys.get(queryKey).getField().getInternalName();
            List<String> fieldNames = index.getFields();
            int fieldIndex = 0;
            for (int size = fieldNames.size(); fieldIndex < size; ++ fieldIndex) {
                if (valueFieldName.equals(fieldNames.get(fieldIndex))) {
                    break;
                }
            }
            field = sqlQuery.aliasedField(alias, sqlIndexTable.getValueField(sqlQuery.database, index, fieldIndex));
        }

        if (comparison != null
                && comparison.isIgnoreCase()
                && (sqlIndex != SqlIndex.STRING
                || sqlIndexTable.getVersion() < 3)) {

            field = DSL.field(sqlQuery.vendor.convertRawToStringSql(sqlQuery.renderContext.render(field)), String.class).lower();
        }

        return (Field<Object>) field;
    }
}
