package com.psddev.dari.db.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.psddev.dari.db.ComparisonPredicate;
import com.psddev.dari.db.CompoundPredicate;
import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Predicate;
import com.psddev.dari.db.PredicateParser;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Region;
import com.psddev.dari.db.Sorter;
import com.psddev.dari.db.UnsupportedIndexException;
import com.psddev.dari.db.UnsupportedPredicateException;
import com.psddev.dari.db.UnsupportedSorterException;
import com.psddev.dari.db.mysql.MySQLDatabase;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.RenderContext;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

class SqlQuery {

    private static final Pattern QUERY_KEY_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    private final AbstractSqlDatabase database;
    private final SqlSchema schema;
    private final Query<?> query;
    private final String aliasPrefix;

    private final SqlVendor vendor;
    private final DSLContext dslContext;
    private final RenderContext renderContext;
    private final Field<UUID> recordIdField;
    private final Field<UUID> recordTypeIdField;
    private final Map<String, Query.MappedKey> mappedKeys;
    private final Map<String, ObjectIndex> selectedIndexes;

    private String fromClause;
    private String whereClause;
    private String havingClause;
    private String orderByClause;
    private final List<Join> joins = new ArrayList<>();
    private final Map<Query<?>, String> subQueries = new LinkedHashMap<>();
    private final Map<Query<?>, SqlQuery> subSqlQueries = new HashMap<>();

    private boolean needsDistinct;
    private Join mysqlIndexHint;
    private boolean mysqlIgnoreIndexPrimaryDisabled;
    private boolean forceLeftJoins;
    private boolean hasAnyLimitingPredicates;

    private final List<Predicate> havingPredicates = new ArrayList<>();
    private final List<Predicate> parentHavingPredicates = new ArrayList<>();

    /**
     * Creates an instance that can translate the given {@code query}
     * with the given {@code database}.
     */
    public SqlQuery(
            AbstractSqlDatabase initialDatabase,
            Query<?> initialQuery,
            String initialAliasPrefix) {

        database = initialDatabase;
        schema = database.schema();
        query = initialQuery;
        aliasPrefix = initialAliasPrefix;

        vendor = database.getVendor();
        dslContext = DSL.using(database.dialect());
        renderContext = dslContext.renderContext().paramType(ParamType.INLINED);
        recordIdField = DSL.field(DSL.name(aliasPrefix + "r", schema.recordId().getName()), schema.uuidDataType());
        recordTypeIdField = DSL.field(DSL.name(aliasPrefix + "r", schema.recordTypeId().getName()), schema.uuidDataType());
        mappedKeys = query.mapEmbeddedKeys(database.getEnvironment());
        selectedIndexes = new HashMap<>();

        for (Map.Entry<String, Query.MappedKey> entry : mappedKeys.entrySet()) {
            selectIndex(entry.getKey(), entry.getValue());
        }
    }

    private void selectIndex(String queryKey, Query.MappedKey mappedKey) {
        ObjectIndex selectedIndex = null;
        int maxMatchCount = 0;

        for (ObjectIndex index : mappedKey.getIndexes()) {
            List<String> indexFields = index.getFields();
            int matchCount = 0;

            for (Query.MappedKey mk : mappedKeys.values()) {
                ObjectField mkf = mk.getField();
                if (mkf != null && indexFields.contains(mkf.getInternalName())) {
                    ++ matchCount;
                }
            }

            if (matchCount > maxMatchCount) {
                selectedIndex = index;
                maxMatchCount = matchCount;
            }
        }

        if (selectedIndex != null) {
            if (maxMatchCount == 1) {
                for (ObjectIndex index : mappedKey.getIndexes()) {
                    if (index.getFields().size() == 1) {
                        selectedIndex = index;
                        break;
                    }
                }
            }

            selectedIndexes.put(queryKey, selectedIndex);
        }
    }

    public SqlQuery(AbstractSqlDatabase initialDatabase, Query<?> initialQuery) {
        this(initialDatabase, initialQuery, "");
    }

    private Field<Object> aliasedField(String alias, String field) {
        return field != null ? DSL.field(DSL.name(aliasPrefix + alias, field)) : null;
    }

    private SqlQuery getOrCreateSubSqlQuery(Query<?> subQuery, boolean forceLeftJoins) {
        SqlQuery subSqlQuery = subSqlQueries.get(subQuery);
        if (subSqlQuery == null) {
            subSqlQuery = new SqlQuery(database, subQuery, aliasPrefix + "s" + subSqlQueries.size());
            subSqlQuery.forceLeftJoins = forceLeftJoins;
            subSqlQuery.initializeClauses();
            subSqlQueries.put(subQuery, subSqlQuery);
        }
        return subSqlQuery;
    }

    /** Initializes FROM, WHERE, and ORDER BY clauses. */
    private void initializeClauses() {
        Set<ObjectType> queryTypes = query.getConcreteTypes(database.getEnvironment());
        String extraJoins = ObjectUtils.to(String.class, query.getOptions().get(AbstractSqlDatabase.EXTRA_JOINS_QUERY_OPTION));

        if (extraJoins != null) {
            Matcher queryKeyMatcher = QUERY_KEY_PATTERN.matcher(extraJoins);
            int lastEnd = 0;
            StringBuilder newExtraJoinsBuilder = new StringBuilder();

            while (queryKeyMatcher.find()) {
                newExtraJoinsBuilder.append(extraJoins.substring(lastEnd, queryKeyMatcher.start()));
                lastEnd = queryKeyMatcher.end();

                String queryKey = queryKeyMatcher.group(1);
                Query.MappedKey mappedKey = query.mapEmbeddedKey(database.getEnvironment(), queryKey);
                mappedKeys.put(queryKey, mappedKey);
                selectIndex(queryKey, mappedKey);
                Join join = getJoin(queryKey);
                join.type = JoinType.LEFT_OUTER;
                newExtraJoinsBuilder.append(renderContext.render(join.getValueField(queryKey, null)));
            }

            newExtraJoinsBuilder.append(extraJoins.substring(lastEnd));
            extraJoins = newExtraJoinsBuilder.toString();
        }

        // Build the WHERE clause.
        Condition whereCondition = query.isFromAll()
                ? DSL.trueCondition()
                : recordTypeIdField.in(query.getConcreteTypeIds(database));

        Predicate predicate = query.getPredicate();

        if (predicate != null) {
            StringBuilder childBuilder = new StringBuilder();
            addWherePredicate(childBuilder, predicate, null, false, true);
            if (childBuilder.length() > 0) {
                whereCondition = whereCondition.and(childBuilder.toString());
            }
        }

        String extraWhere = ObjectUtils.to(String.class, query.getOptions().get(AbstractSqlDatabase.EXTRA_WHERE_QUERY_OPTION));

        if (!ObjectUtils.isBlank(extraWhere)) {
            whereCondition = whereCondition.and(extraWhere);
        }

        // Builds the ORDER BY clause.
        StringBuilder orderByBuilder = new StringBuilder();

        for (Sorter sorter : query.getSorters()) {
            addOrderByClause(orderByBuilder, sorter);
        }

        if (orderByBuilder.length() > 0) {
            orderByBuilder.setLength(orderByBuilder.length() - 2);
            orderByBuilder.insert(0, "\nORDER BY ");
        }

        // Builds the FROM clause.
        StringBuilder fromBuilder = new StringBuilder();

        for (Join join : joins) {

            if (join.indexKeys.isEmpty()) {
                continue;
            }

            // e.g. JOIN RecordIndex AS i#
            fromBuilder.append('\n');
            fromBuilder.append((forceLeftJoins ? JoinType.LEFT_OUTER : join.type).token);
            fromBuilder.append(' ');
            fromBuilder.append(dslContext.renderContext().declareTables(true).render(join.table));

            if (join.type == JoinType.INNER && join.equals(mysqlIndexHint)) {
                fromBuilder.append(" /*! USE INDEX (k_name_value) */");

            } else if (join.tableName != null && join.tableName.startsWith("RecordLocation")) {
                fromBuilder.append(" /*! IGNORE INDEX (PRIMARY) */");
            }

            fromBuilder.append(" ON ");

            Condition joinCondition = join.idField.eq(recordIdField)
                    .and(join.typeIdField.eq(recordTypeIdField))
                    .and(join.keyField.in(join.indexKeys.stream()
                            .map(database::getSymbolId)
                            .collect(Collectors.toSet())));

            fromBuilder.append(renderContext.render(joinCondition));
        }

        for (Map.Entry<Query<?>, String> entry : subQueries.entrySet()) {
            Query<?> subQuery = entry.getKey();
            SqlQuery subSqlQuery = getOrCreateSubSqlQuery(subQuery, false);

            if (subSqlQuery.needsDistinct) {
                needsDistinct = true;
            }

            fromBuilder.append("\nINNER JOIN ");
            vendor.appendIdentifier(fromBuilder, "Record");
            fromBuilder.append(' ');
            fromBuilder.append(subSqlQuery.aliasPrefix);
            fromBuilder.append("r ON ");
            fromBuilder.append(entry.getValue());
            fromBuilder.append(subSqlQuery.aliasPrefix);
            fromBuilder.append("r.");
            vendor.appendIdentifier(fromBuilder, "id");
            fromBuilder.append(subSqlQuery.fromClause);
        }

        if (extraJoins != null) {
            fromBuilder.append(' ');
            fromBuilder.append(extraJoins);
        }

        this.whereClause = "\nWHERE " + renderContext.render(whereCondition);

        StringBuilder havingBuilder = new StringBuilder();
        if (hasDeferredHavingPredicates()) {
            StringBuilder childBuilder = new StringBuilder();
            int i = 0;
            for (Predicate havingPredicate : havingPredicates) {
                addWherePredicate(childBuilder, havingPredicate, parentHavingPredicates.get(i++), false, false);
            }
            if (childBuilder.length() > 0) {
                havingBuilder.append(" \nHAVING ");
                havingBuilder.append(childBuilder);
            }
        }

        String extraHaving = ObjectUtils.to(String.class, query.getOptions().get(AbstractSqlDatabase.EXTRA_HAVING_QUERY_OPTION));
        havingBuilder.append(ObjectUtils.isBlank(extraHaving) ? "" : ("\n" + (ObjectUtils.isBlank(this.havingClause) ? "HAVING" : "AND") + " " + extraHaving));
        this.havingClause = havingBuilder.toString();

        this.orderByClause = orderByBuilder.toString();
        this.fromClause = fromBuilder.toString();

    }

    /** Adds the given {@code predicate} to the {@code WHERE} clause. */
    private void addWherePredicate(
            StringBuilder whereBuilder,
            Predicate predicate,
            Predicate parentPredicate,
            boolean usesLeftJoin,
            boolean deferMetricAndHavingPredicates) {

        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            String operator = compoundPredicate.getOperator();
            boolean isNot = PredicateParser.NOT_OPERATOR.equals(operator);

            // e.g. (child1) OR (child2) OR ... (child#)
            if (isNot || PredicateParser.OR_OPERATOR.equals(operator)) {
                Condition compoundCondition = null;
                List<Predicate> children = compoundPredicate.getChildren();

                boolean usesLeftJoinChildren;
                if (children.size() > 1) {
                    usesLeftJoinChildren = true;
                    needsDistinct = true;
                } else {
                    usesLeftJoinChildren = isNot;
                }

                for (Predicate child : children) {
                    StringBuilder childBuilder = new StringBuilder();
                    addWherePredicate(childBuilder, child, predicate, usesLeftJoinChildren, deferMetricAndHavingPredicates);
                    if (childBuilder.length() > 0) {
                        compoundCondition = compoundCondition != null
                                ? compoundCondition.or(childBuilder.toString())
                                : DSL.condition(childBuilder.toString());
                    }
                }

                if (compoundCondition != null) {
                    whereBuilder.append(
                            renderContext.render(isNot
                                    ? compoundCondition.not()
                                    : compoundCondition));
                }

                return;

            // e.g. (child1) AND (child2) AND .... (child#)
            } else if (PredicateParser.AND_OPERATOR.equals(operator)) {
                Condition compoundCondition = null;

                for (Predicate child : compoundPredicate.getChildren()) {
                    StringBuilder childBuilder = new StringBuilder();
                    addWherePredicate(childBuilder, child, predicate, usesLeftJoin, deferMetricAndHavingPredicates);
                    if (childBuilder.length() > 0) {
                        compoundCondition = compoundCondition != null
                                ? compoundCondition.and(childBuilder.toString())
                                : DSL.condition(childBuilder.toString());
                    }
                }

                if (compoundCondition != null) {
                    whereBuilder.append(renderContext.render(compoundCondition));
                }

                return;
            }

        } else if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate comparisonPredicate = (ComparisonPredicate) predicate;
            String queryKey = comparisonPredicate.getKey();
            Query.MappedKey mappedKey = mappedKeys.get(queryKey);
            boolean isFieldCollection = mappedKey.isInternalCollectionType();

            Join join = null;
            if (mappedKey.getField() != null
                    && parentPredicate instanceof CompoundPredicate
                    && PredicateParser.OR_OPERATOR.equals(parentPredicate.getOperator())) {
                for (Join j : joins) {
                    if (j.parent == parentPredicate
                            && j.sqlIndexTable.equals(schema.findSelectIndexTable(mappedKeys.get(queryKey).getInternalType()))) {
                        join = j;
                        join.addIndexKey(queryKey);
                        needsDistinct = true;
                        break;
                    }
                }
                if (join == null) {
                    join = getJoin(queryKey);
                    join.parent = parentPredicate;
                }

            } else if (isFieldCollection) {
                join = createJoin(queryKey);

            } else {
                join = getJoin(queryKey);
            }

            if (usesLeftJoin) {
                join.type = JoinType.LEFT_OUTER;
            }

            if (isFieldCollection && join.sqlIndexTable == null) {
                needsDistinct = true;
            }

            if (deferMetricAndHavingPredicates) {
                if (join.isHaving) {
                    // pass for now; we'll get called again later.
                    havingPredicates.add(predicate);
                    parentHavingPredicates.add(parentPredicate);
                    return;
                }
            }

            Field<Object> joinValueField = join.getValueField(queryKey, comparisonPredicate);
            String operator = comparisonPredicate.getOperator();
            StringBuilder comparisonBuilder = new StringBuilder();
            boolean hasMissing = false;
            int subClauseCount = 0;
            boolean isNotEqualsAll = PredicateParser.NOT_EQUALS_ALL_OPERATOR.equals(operator);
            if (!isNotEqualsAll) {
                hasAnyLimitingPredicates = true;
            }

            if (isNotEqualsAll || PredicateParser.EQUALS_ANY_OPERATOR.equals(operator)) {
                Query<?> valueQuery = mappedKey.getSubQueryWithComparison(comparisonPredicate);

                // e.g. field IN (SELECT ...)
                if (valueQuery != null) {
                    if (isNotEqualsAll || isFieldCollection) {
                        needsDistinct = true;
                    }

                    if (findSimilarComparison(mappedKey.getField(), query.getPredicate())) {
                        Table<?> subQueryTable = DSL.table(new SqlQuery(database, valueQuery).subQueryStatement());
                        Condition subQueryCondition = isNotEqualsAll
                                ? joinValueField.notIn(subQueryTable)
                                : joinValueField.in(subQueryTable);

                        whereBuilder.append(renderContext.render(subQueryCondition));

                    } else {
                        SqlQuery subSqlQuery = getOrCreateSubSqlQuery(valueQuery, join.type == JoinType.LEFT_OUTER);
                        subQueries.put(valueQuery, renderContext.render(joinValueField) + (isNotEqualsAll ? " != " : " = "));
                        whereBuilder.append(subSqlQuery.whereClause.substring(7));
                    }

                    return;
                }

                List<Object> inValues = new ArrayList<>();

                for (Object value : comparisonPredicate.resolveValues(database)) {
                    boolean isInValue = false;

                    if (value == null) {
                        ++ subClauseCount;
                        comparisonBuilder.append(renderContext.render(DSL.falseCondition()));

                    } else if (value == Query.MISSING_VALUE) {
                        ++ subClauseCount;
                        hasMissing = true;

                        if (isNotEqualsAll) {
                            if (isFieldCollection) {
                                needsDistinct = true;
                            }

                            comparisonBuilder.append(renderContext.render(joinValueField.isNotNull()));

                        } else {
                            join.type = JoinType.LEFT_OUTER;

                            comparisonBuilder.append(renderContext.render(joinValueField.isNull()));
                        }

                    } else if (value instanceof Region) {
                        if (!database.isIndexSpatial()) {
                            throw new UnsupportedOperationException();
                        }

                        List<Location> locations = ((Region) value).getLocations();
                        if (!locations.isEmpty()) {
                            ++ subClauseCount;

                            if (isNotEqualsAll) {
                                comparisonBuilder.append("NOT ");
                            }

                            try {
                                vendor.appendWhereRegion(comparisonBuilder, (Region) value, renderContext.render(joinValueField));

                            } catch (UnsupportedIndexException uie) {
                                throw new UnsupportedIndexException(vendor, queryKey);
                            }
                        }

                    } else {
                        if (!database.isIndexSpatial() && value instanceof Location) {
                            throw new UnsupportedOperationException();
                        }

                        Object convertedValue = join.convertValue(comparisonPredicate, value);

                        ++ subClauseCount;

                        if (isNotEqualsAll) {
                            join.type = JoinType.LEFT_OUTER;
                            needsDistinct = true;
                            hasMissing = true;

                            comparisonBuilder.append('(');
                            comparisonBuilder.append(
                                    renderContext.render(
                                            joinValueField.isNull().or(
                                                    joinValueField.ne(convertedValue))));
                            comparisonBuilder.append(')');

                        } else {
                            if (join.likeValuePrefix != null) {
                                comparisonBuilder.append(joinValueField);
                                comparisonBuilder.append(" LIKE ");
                                join.appendValue(comparisonBuilder, comparisonPredicate, join.likeValuePrefix + database.getReadSymbolId(value.toString()) + ";%");

                            } else {
                                isInValue = true;

                                inValues.add(convertedValue);
                            }
                        }
                    }

                    if (!isInValue) {
                        comparisonBuilder.append(isNotEqualsAll ? " AND " : " OR  ");
                    }
                }

                if (!inValues.isEmpty()) {
                    comparisonBuilder.append(
                            renderContext.render(
                                    joinValueField.in(inValues)));
                    comparisonBuilder.append(isNotEqualsAll ? " AND " : " OR  ");
                }

                if (comparisonBuilder.length() == 0) {
                    whereBuilder.append(isNotEqualsAll ? "1 = 1" : "0 = 1");
                    return;
                }

            } else {
                boolean isStartsWith = PredicateParser.STARTS_WITH_OPERATOR.equals(operator);
                boolean isContains = PredicateParser.CONTAINS_OPERATOR.equals(operator);
                String sqlOperator = isStartsWith ? "LIKE"
                        : isContains ? "LIKE"
                        : PredicateParser.LESS_THAN_OPERATOR.equals(operator) ? "<"
                        : PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR.equals(operator) ? "<="
                        : PredicateParser.GREATER_THAN_OPERATOR.equals(operator) ? ">"
                        : PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR.equals(operator) ? ">="
                        : null;

                Query<?> valueQuery = mappedKey.getSubQueryWithComparison(comparisonPredicate);

                // e.g. field startsWith (SELECT ...)
                if (valueQuery != null) {
                    if (isFieldCollection) {
                        needsDistinct = true;
                    }

                    if (findSimilarComparison(mappedKey.getField(), query.getPredicate())) {
                        Table<?> subQueryTable = DSL.table(new SqlQuery(database, valueQuery).subQueryStatement());
                        Condition subQueryCondition = joinValueField.in(subQueryTable);

                        whereBuilder.append(renderContext.render(subQueryCondition));

                    } else {
                        SqlQuery subSqlQuery = getOrCreateSubSqlQuery(valueQuery, join.type == JoinType.LEFT_OUTER);
                        subQueries.put(valueQuery, renderContext.render(joinValueField) + " = ");
                        whereBuilder.append(subSqlQuery.whereClause.substring(7));
                    }

                    return;
                }

                // e.g. field OP value1 OR field OP value2 OR ... field OP value#
                if (sqlOperator != null) {
                    for (Object value : comparisonPredicate.resolveValues(database)) {
                        ++ subClauseCount;

                        if (value == null) {
                            comparisonBuilder.append(renderContext.render(DSL.falseCondition()));

                        } else if (value instanceof Location) {
                            if (!database.isIndexSpatial()) {
                                throw new UnsupportedOperationException();
                            }

                            ++ subClauseCount;

                            if (isNotEqualsAll) {
                                comparisonBuilder.append("NOT ");
                            }

                            try {
                                vendor.appendWhereLocation(comparisonBuilder, (Location) value, renderContext.render(joinValueField));

                            } catch (UnsupportedIndexException uie) {
                                throw new UnsupportedIndexException(vendor, queryKey);
                            }

                        } else if (value == Query.MISSING_VALUE) {
                            hasMissing = true;
                            join.type = JoinType.LEFT_OUTER;

                            comparisonBuilder.append(renderContext.render(joinValueField.isNull()));

                        } else {
                            comparisonBuilder.append(renderContext.render(joinValueField));
                            comparisonBuilder.append(' ');
                            comparisonBuilder.append(sqlOperator);
                            comparisonBuilder.append(' ');
                            if (isStartsWith) {
                                value = value.toString() + "%";
                            } else if (isContains) {
                                value = "%" + value.toString() + "%";
                            }
                            join.appendValue(comparisonBuilder, comparisonPredicate, value);
                        }

                        comparisonBuilder.append(" OR  ");
                    }

                    if (comparisonBuilder.length() == 0) {
                        whereBuilder.append(renderContext.render(DSL.falseCondition()));
                        return;
                    }
                }
            }

            if (comparisonBuilder.length() > 0) {
                comparisonBuilder.setLength(comparisonBuilder.length() - 5);

                if (!hasMissing) {
                    if (join.needsIndexTable) {
                        String indexKey = mappedKeys.get(queryKey).getIndexKey(selectedIndexes.get(queryKey));
                        if (indexKey != null) {
                            whereBuilder.append(renderContext.render(join.keyField));
                            whereBuilder.append(" = ");
                            whereBuilder.append(join.quoteIndexKey(indexKey));
                            whereBuilder.append(" AND ");
                        }
                    }

                    if (join.needsIsNotNull) {
                        whereBuilder.append(renderContext.render(joinValueField.isNotNull()));
                        whereBuilder.append(" AND ");
                    }

                    if (subClauseCount > 1) {
                        needsDistinct = true;
                        whereBuilder.append('(');
                        comparisonBuilder.append(')');
                    }
                }

                whereBuilder.append(comparisonBuilder);
                return;
            }
        }

        throw new UnsupportedPredicateException(this, predicate);
    }

    private void addOrderByClause(StringBuilder orderByBuilder, Sorter sorter) {

        String operator = sorter.getOperator();
        boolean ascending = Sorter.ASCENDING_OPERATOR.equals(operator);
        boolean descending = Sorter.DESCENDING_OPERATOR.equals(operator);
        boolean closest = Sorter.CLOSEST_OPERATOR.equals(operator);
        boolean farthest = Sorter.FARTHEST_OPERATOR.equals(operator);

        if (ascending || descending || closest || farthest) {
            String queryKey = (String) sorter.getOptions().get(0);
            Join join = getSortFieldJoin(queryKey);
            String joinValueFieldString = renderContext.render(join.getValueField(queryKey, null));
            Query<?> subQuery = mappedKeys.get(queryKey).getSubQueryWithSorter(sorter, 0);

            if (subQuery != null) {
                SqlQuery subSqlQuery = getOrCreateSubSqlQuery(subQuery, true);
                subQueries.put(subQuery, joinValueFieldString + " = ");
                orderByBuilder.append(subSqlQuery.orderByClause.substring(9));
                orderByBuilder.append(", ");
                return;
            }

            if (ascending || descending) {
                orderByBuilder.append(joinValueFieldString);

            } else {
                if (!database.isIndexSpatial()) {
                    throw new UnsupportedOperationException();
                }

                Location location = (Location) sorter.getOptions().get(1);

                StringBuilder selectBuilder = new StringBuilder();

                try {
                    vendor.appendNearestLocation(orderByBuilder, selectBuilder, location, joinValueFieldString);

                } catch (UnsupportedIndexException uie) {
                    throw new UnsupportedIndexException(vendor, queryKey);
                }
            }

            orderByBuilder.append(' ');
            orderByBuilder.append(ascending || closest ? "ASC" : "DESC");
            orderByBuilder.append(", ");
            return;
        }

        throw new UnsupportedSorterException(database, sorter);
    }

    private boolean findSimilarComparison(ObjectField field, Predicate predicate) {
        if (field != null) {
            if (predicate instanceof CompoundPredicate) {
                for (Predicate child : ((CompoundPredicate) predicate).getChildren()) {
                    if (findSimilarComparison(field, child)) {
                        return true;
                    }
                }

            } else if (predicate instanceof ComparisonPredicate) {
                ComparisonPredicate comparison = (ComparisonPredicate) predicate;
                Query.MappedKey mappedKey = mappedKeys.get(comparison.getKey());

                if (field.equals(mappedKey.getField())
                        && mappedKey.getSubQueryWithComparison(comparison) == null) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean hasDeferredHavingPredicates() {
        return !havingPredicates.isEmpty();
    }

    /**
     * Returns an SQL statement that can be used to get a count
     * of all rows matching the query.
     */
    public String countStatement() {
        StringBuilder statementBuilder = new StringBuilder();
        initializeClauses();

        statementBuilder.append("SELECT COUNT(");
        if (needsDistinct) {
            statementBuilder.append("DISTINCT ");
        }
        statementBuilder.append(renderContext.render(recordIdField));
        statementBuilder.append(')');

        statementBuilder.append(" \nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');
        statementBuilder.append(fromClause.replace(" /*! USE INDEX (k_name_value) */", ""));
        statementBuilder.append(whereClause);
        return statementBuilder.toString();
    }

    /**
     * Returns an SQL statement that can be used to delete all rows
     * matching the query.
     */
    public String deleteStatement() {
        StringBuilder statementBuilder = new StringBuilder();
        initializeClauses();
        statementBuilder.append("DELETE r\nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');
        statementBuilder.append(fromClause);
        statementBuilder.append(whereClause);
        statementBuilder.append(havingClause);
        statementBuilder.append(orderByClause);

        return statementBuilder.toString();
    }

    /**
     * Returns an SQL statement that can be used to get all objects
     * grouped by the values of the given {@code groupFields}.
     */
    public String groupStatement(String[] groupFields) {
        Map<String, Join> groupJoins = new LinkedHashMap<>();
        Map<String, SqlQuery> groupSubSqlQueries = new HashMap<>();
        if (groupFields != null) {
            for (String groupField : groupFields) {
                Query.MappedKey mappedKey = query.mapEmbeddedKey(database.getEnvironment(), groupField);
                mappedKeys.put(groupField, mappedKey);
                Iterator<ObjectIndex> indexesIterator = mappedKey.getIndexes().iterator();
                if (indexesIterator.hasNext()) {
                    ObjectIndex selectedIndex = indexesIterator.next();
                    while (indexesIterator.hasNext()) {
                        ObjectIndex index = indexesIterator.next();
                        if (selectedIndex.getFields().size() < index.getFields().size()) {
                            selectedIndex = index;
                        }
                    }
                    selectedIndexes.put(groupField, selectedIndex);
                }
                Join join = getJoin(groupField);
                Query<?> subQuery = mappedKey.getSubQueryWithGroupBy();
                if (subQuery != null) {
                    SqlQuery subSqlQuery = getOrCreateSubSqlQuery(subQuery, true);
                    groupSubSqlQueries.put(groupField, subSqlQuery);
                    subQueries.put(subQuery, renderContext.render(join.getValueField(groupField, null)) + " = ");
                }
                groupJoins.put(groupField, join);
            }
        }

        StringBuilder statementBuilder = new StringBuilder();
        StringBuilder groupBy = new StringBuilder();
        initializeClauses();

        statementBuilder.append("SELECT COUNT(");
        if (needsDistinct) {
            statementBuilder.append("DISTINCT ");
        }
        statementBuilder.append(renderContext.render(recordIdField));
        statementBuilder.append(')');
        statementBuilder.append(' ');
        vendor.appendIdentifier(statementBuilder, "_count");
        int columnNum = 0;
        for (Map.Entry<String, Join> entry : groupJoins.entrySet()) {
            statementBuilder.append(", ");
            if (!groupSubSqlQueries.containsKey(entry.getKey())) {
                statementBuilder.append(renderContext.render(entry.getValue().getValueField(entry.getKey(), null)));
            }
            statementBuilder.append(' ');
            String columnAlias = null;
            if (!entry.getValue().queryKey.equals(Query.ID_KEY) && !entry.getValue().queryKey.equals(Query.DIMENSION_KEY)) { // Special case for id and dimensionId
                // These column names just need to be unique if we put this statement in a subquery
                columnAlias = "value" + columnNum;
            }
            ++columnNum;
            if (columnAlias != null) {
                vendor.appendIdentifier(statementBuilder, columnAlias);
            }
        }

        String selectClause = statementBuilder.toString();

        statementBuilder.append("\nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');
        statementBuilder.append(fromClause.replace(" /*! USE INDEX (k_name_value) */", ""));
        statementBuilder.append(whereClause);

        for (Map.Entry<String, Join> entry : groupJoins.entrySet()) {
            if (!groupSubSqlQueries.containsKey(entry.getKey())) {
                groupBy.append(renderContext.render(entry.getValue().getValueField(entry.getKey(), null)));
            }
            groupBy.append(", ");
        }

        if (groupBy.length() > 0) {
            groupBy.setLength(groupBy.length() - 2);
            groupBy.insert(0, " GROUP BY ");
        }

        String groupByClause = groupBy.toString();

        statementBuilder.append(groupByClause);

        statementBuilder.append(havingClause);
        statementBuilder.append(orderByClause);

        return statementBuilder.toString();
    }

    /**
     * Returns an SQL statement that can be used to get when the rows
     * matching the query were last updated.
     */
    public String lastUpdateStatement() {
        StringBuilder statementBuilder = new StringBuilder();
        initializeClauses();

        statementBuilder.append("SELECT MAX(r.");
        vendor.appendIdentifier(statementBuilder, "updateDate");
        statementBuilder.append(")\nFROM ");
        vendor.appendIdentifier(statementBuilder, "RecordUpdate");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');
        statementBuilder.append(fromClause);
        statementBuilder.append(whereClause);

        return statementBuilder.toString();
    }

    /**
     * Returns an SQL statement that can be used to list all rows
     * matching the query.
     */
    public String selectStatement() {
        StringBuilder statementBuilder = new StringBuilder();
        initializeClauses();

        statementBuilder.append("SELECT");
        if (needsDistinct && vendor.supportsDistinctBlob()) {
            statementBuilder.append(" DISTINCT");
        }

        statementBuilder.append(" r.");
        vendor.appendIdentifier(statementBuilder, "id");
        statementBuilder.append(", r.");
        vendor.appendIdentifier(statementBuilder, "typeId");

        List<String> fields = query.getFields();
        if (fields == null) {
            if (!needsDistinct || vendor.supportsDistinctBlob()) {
                statementBuilder.append(", r.");
                vendor.appendIdentifier(statementBuilder, "data");
            }
        } else if (!fields.isEmpty()) {
            statementBuilder.append(", ");
            vendor.appendSelectFields(statementBuilder, fields);
        }

        String extraColumns = ObjectUtils.to(String.class, query.getOptions().get(AbstractSqlDatabase.EXTRA_COLUMNS_QUERY_OPTION));

        if (extraColumns != null) {
            statementBuilder.append(", ");
            statementBuilder.append(extraColumns);
        }

        if (!needsDistinct && !subSqlQueries.isEmpty()) {
            for (Map.Entry<Query<?>, SqlQuery> entry : subSqlQueries.entrySet()) {
                SqlQuery subSqlQuery = entry.getValue();
                statementBuilder.append(", " + subSqlQuery.aliasPrefix + "r." + AbstractSqlDatabase.ID_COLUMN + " AS " + AbstractSqlDatabase.SUB_DATA_COLUMN_ALIAS_PREFIX + subSqlQuery.aliasPrefix + "_" + AbstractSqlDatabase.ID_COLUMN);
                statementBuilder.append(", " + subSqlQuery.aliasPrefix + "r." + AbstractSqlDatabase.TYPE_ID_COLUMN + " AS " + AbstractSqlDatabase.SUB_DATA_COLUMN_ALIAS_PREFIX + subSqlQuery.aliasPrefix + "_" + AbstractSqlDatabase.TYPE_ID_COLUMN);
                statementBuilder.append(", " + subSqlQuery.aliasPrefix + "r." + AbstractSqlDatabase.DATA_COLUMN + " AS " + AbstractSqlDatabase.SUB_DATA_COLUMN_ALIAS_PREFIX + subSqlQuery.aliasPrefix + "_" + AbstractSqlDatabase.DATA_COLUMN);
            }
        }

        statementBuilder.append("\nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');

        if (fromClause.length() > 0
                && !fromClause.contains("LEFT OUTER JOIN")
                && hasAnyLimitingPredicates
                && !mysqlIgnoreIndexPrimaryDisabled) {
            statementBuilder.append(" /*! IGNORE INDEX (PRIMARY) */");
        }

        statementBuilder.append(fromClause);
        statementBuilder.append(whereClause);
        statementBuilder.append(havingClause);
        statementBuilder.append(orderByClause);

        if (needsDistinct && !vendor.supportsDistinctBlob()) {
            StringBuilder distinctBuilder = new StringBuilder();

            distinctBuilder.append("SELECT");
            distinctBuilder.append(" r.");
            vendor.appendIdentifier(distinctBuilder, "id");
            distinctBuilder.append(", r.");
            vendor.appendIdentifier(distinctBuilder, "typeId");

            if (fields == null) {
                distinctBuilder.append(", r.");
                vendor.appendIdentifier(distinctBuilder, "data");
            } else if (!fields.isEmpty()) {
                distinctBuilder.append(", ");
                vendor.appendSelectFields(distinctBuilder, fields);
            }

            distinctBuilder.append(" FROM ");
            vendor.appendIdentifier(distinctBuilder, AbstractSqlDatabase.RECORD_TABLE);
            distinctBuilder.append(" r INNER JOIN (");
            distinctBuilder.append(statementBuilder.toString());
            distinctBuilder.append(") d0 ON (r.id = d0.id)");

            statementBuilder = distinctBuilder;
        }

        return statementBuilder.toString();
    }

    /** Returns an SQL statement that can be used as a sub-query. */
    public String subQueryStatement() {
        StringBuilder statementBuilder = new StringBuilder();
        initializeClauses();

        statementBuilder.append("SELECT");
        if (needsDistinct) {
            statementBuilder.append(" DISTINCT");
        }
        statementBuilder.append(" r.");
        vendor.appendIdentifier(statementBuilder, "id");
        statementBuilder.append("\nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');

        statementBuilder.append(fromClause);
        statementBuilder.append(whereClause);
        statementBuilder.append(havingClause);
        statementBuilder.append(orderByClause);

        return statementBuilder.toString();
    }

    private enum JoinType {

        INNER("INNER JOIN"),
        LEFT_OUTER("LEFT OUTER JOIN");

        public final String token;

        JoinType(String token) {
            this.token = token;
        }
    }

    private Join createJoin(String queryKey) {
        String alias = "i" + joins.size();
        Join join = new Join(alias, queryKey);
        joins.add(join);
        if (queryKey.equals(query.getOptions().get(MySQLDatabase.MYSQL_INDEX_HINT_QUERY_OPTION))) {
            mysqlIndexHint = join;
        }
        return join;
    }

    /** Returns the column alias for the given {@code queryKey}. */
    private Join getJoin(String queryKey) {
        ObjectIndex index = selectedIndexes.get(queryKey);
        for (Join join : joins) {
            if (queryKey.equals(join.queryKey)) {
                return join;
            } else {
                String indexKey = mappedKeys.get(queryKey).getIndexKey(index);
                if (indexKey != null
                        && indexKey.equals(mappedKeys.get(join.queryKey).getIndexKey(join.index))
                        && ((mappedKeys.get(queryKey).getHashAttribute() != null && mappedKeys.get(queryKey).getHashAttribute().equals(join.hashAttribute))
                        || (mappedKeys.get(queryKey).getHashAttribute() == null && join.hashAttribute == null))) {
                    // If there's a #attribute on the mapped key, make sure we are returning the matching join.
                    return join;
                }
            }
        }
        return createJoin(queryKey);
    }

    /** Returns the column alias for the given field-based {@code sorter}. */
    private Join getSortFieldJoin(String queryKey) {
        ObjectIndex index = selectedIndexes.get(queryKey);
        for (Join join : joins) {
            if (queryKey.equals(join.queryKey)) {
                return join;
            } else {
                String indexKey = mappedKeys.get(queryKey).getIndexKey(index);
                if (indexKey != null
                        && indexKey.equals(mappedKeys.get(join.queryKey).getIndexKey(join.index))
                        && ((mappedKeys.get(queryKey).getHashAttribute() != null && mappedKeys.get(queryKey).getHashAttribute().equals(join.hashAttribute))
                        || (mappedKeys.get(queryKey).getHashAttribute() == null && join.hashAttribute == null))) {
                    // If there's a #attribute on the mapped key, make sure we are returning the matching join.
                    return join;
                }
            }
        }

        Join join = createJoin(queryKey);
        join.type = JoinType.LEFT_OUTER;
        return join;
    }

    public String getAliasPrefix() {
        return aliasPrefix;
    }

    private class Join {

        public Predicate parent;
        public JoinType type = JoinType.INNER;

        public final boolean needsIndexTable;
        public final boolean needsIsNotNull;
        public final String likeValuePrefix;
        public final String queryKey;
        public final String indexType;
        public final Table<?> table;
        public final Field<Object> idField;
        public final Field<Object> typeIdField;
        public final Field<Object> keyField;
        public final List<String> indexKeys = new ArrayList<>();

        private final String alias;
        private final String tableName;
        private final ObjectIndex index;
        private final AbstractSqlIndex sqlIndexTable;
        private final Field<?> valueField;
        private final String hashAttribute;
        private final boolean isHaving;

        public Join(String alias, String queryKey) {
            this.alias = alias;
            this.queryKey = queryKey;

            Query.MappedKey mappedKey = mappedKeys.get(queryKey);
            this.hashAttribute = mappedKey.getHashAttribute();
            this.index = selectedIndexes.get(queryKey);

            this.indexType = mappedKey.getInternalType();

            if (Query.ID_KEY.equals(queryKey)) {
                needsIndexTable = false;
                likeValuePrefix = null;
                valueField = recordIdField;
                sqlIndexTable = null;
                table = null;
                tableName = null;
                idField = null;
                typeIdField = null;
                keyField = null;
                needsIsNotNull = true;
                isHaving = false;

            } else if (Query.TYPE_KEY.equals(queryKey)) {
                needsIndexTable = false;
                likeValuePrefix = null;
                valueField = recordTypeIdField;
                sqlIndexTable = null;
                table = null;
                tableName = null;
                idField = null;
                typeIdField = null;
                keyField = null;
                needsIsNotNull = true;
                isHaving = false;

            } else if (Query.COUNT_KEY.equals(queryKey)) {
                needsIndexTable = false;
                likeValuePrefix = null;
                valueField = recordIdField.count();
                sqlIndexTable = null;
                table = null;
                tableName = null;
                idField = null;
                typeIdField = null;
                keyField = null;
                needsIsNotNull = false;
                isHaving = true;

            } else if (Query.ANY_KEY.equals(queryKey)
                    || Query.LABEL_KEY.equals(queryKey)) {
                throw new UnsupportedIndexException(database, queryKey);

            } else {
                needsIndexTable = true;
                likeValuePrefix = null;
                addIndexKey(queryKey);
                valueField = null;
                sqlIndexTable = schema.findSelectIndexTable(index);

                tableName = sqlIndexTable.table().getName();
                table = DSL.table(DSL.name(tableName)).as(aliasPrefix + alias);

                idField = aliasedField(alias, sqlIndexTable.id().getName());
                typeIdField = aliasedField(alias, sqlIndexTable.typeId().getName());
                keyField = aliasedField(alias, sqlIndexTable.symbolId().getName());
                needsIsNotNull = true;
                isHaving = false;
            }
        }

        public String getAlias() {
            return this.alias;
        }

        public String toString() {
            return this.tableName + " (" + this.alias + ") ." + renderContext.render(this.valueField);
        }

        public String getTableName() {
            return this.tableName;
        }

        public void addIndexKey(String queryKey) {
            String indexKey = mappedKeys.get(queryKey).getIndexKey(selectedIndexes.get(queryKey));
            if (ObjectUtils.isBlank(indexKey)) {
                throw new UnsupportedIndexException(database, indexKey);
            }
            if (needsIndexTable) {
                indexKeys.add(indexKey);
            }
        }

        public Object quoteIndexKey(String indexKey) {
            return AbstractSqlDatabase.quoteValue(database.getReadSymbolId(indexKey));
        }

        public Object convertValue(ComparisonPredicate comparison, Object value) {
            Query.MappedKey mappedKey = mappedKeys.get(comparison.getKey());
            ObjectField field = mappedKey.getField();
            ObjectIndex index = selectedIndexes.get(queryKey);
            AbstractSqlIndex fieldSqlIndexTable = field != null
                    ? schema.findSelectIndexTable(field.getInternalItemType())
                    : sqlIndexTable;

            String tableName = fieldSqlIndexTable != null ? fieldSqlIndexTable.table().getName() : null;

            if (tableName != null && tableName.startsWith("RecordUuid")) {
                value = ObjectUtils.to(UUID.class, value);

            } else if (tableName != null && tableName.startsWith("RecordNumber")
                    && !PredicateParser.STARTS_WITH_OPERATOR.equals(comparison.getOperator())) {
                if (value != null) {
                    Long valueLong = ObjectUtils.to(Long.class, value);
                    if (valueLong != null) {
                        value = valueLong;
                    } else {
                        value = ObjectUtils.to(Double.class, value);
                    }
                }

            } else if (tableName != null && tableName.startsWith("RecordString")) {
                if (comparison.isIgnoreCase()) {
                    value = value.toString().toLowerCase(Locale.ENGLISH);
                } else if (database.comparesIgnoreCase()) {
                    String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());
                    if (!index.isCaseSensitive()) {
                        valueString = valueString.toLowerCase(Locale.ENGLISH);
                    }
                    value = valueString;
                }
            }

            return value;
        }

        public void appendValue(StringBuilder builder, ComparisonPredicate comparison, Object value) {
            vendor.appendValue(builder, convertValue(comparison, value));
        }

        public Field<Object> getValueField(String queryKey, ComparisonPredicate comparison) {
            Field<?> field;

            if (valueField != null) {
                field = valueField;

            } else {
                field = aliasedField(alias, sqlIndexTable.value().getName());
            }

            return (Field<Object>) field;
        }
    }
}
