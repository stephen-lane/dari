package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.RenderContext;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/** Internal representation of an SQL query based on a Dari one. */
class SqlQuery {

    private static final Pattern QUERY_KEY_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");
    //private static final Logger LOGGER = LoggerFactory.getLogger(SqlQuery.class);

    private final SqlDatabase database;
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
    private Condition whereCondition;
    private String havingClause;
    private String orderByClause;
    private final List<Join> joins = new ArrayList<>();
    private final Map<Query<?>, String> subQueries = new CompactMap<>();
    private final Map<Query<?>, SqlQuery> subSqlQueries = new HashMap<>();

    private boolean needsDistinct;
    private Join mysqlIndexHint;
    private boolean mysqlIgnoreIndexPrimaryDisabled;
    private boolean forceLeftJoins;

    /**
     * Creates an instance that can translate the given {@code query}
     * with the given {@code database}.
     */
    public SqlQuery(
            SqlDatabase initialDatabase,
            Query<?> initialQuery,
            String initialAliasPrefix) {

        database = initialDatabase;
        query = initialQuery;
        aliasPrefix = initialAliasPrefix;

        vendor = database.getVendor();
        dslContext = DSL.using(SQLDialect.MYSQL);
        renderContext = dslContext.renderContext().paramType(ParamType.INLINED);
        recordIdField = DSL.field(DSL.name(aliasPrefix + "r", SqlDatabase.ID_COLUMN), vendor.uuidDataType());
        recordTypeIdField = DSL.field(DSL.name(aliasPrefix + "r", SqlDatabase.TYPE_ID_COLUMN), vendor.uuidDataType());
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

    public SqlQuery(SqlDatabase initialDatabase, Query<?> initialQuery) {
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
        String extraJoins = ObjectUtils.to(String.class, query.getOptions().get(SqlDatabase.EXTRA_JOINS_QUERY_OPTION));

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

        // Builds the WHERE clause.
        Condition whereCondition = query.isFromAll()
                ? DSL.trueCondition()
                : recordTypeIdField.in(query.getConcreteTypeIds(database));

        Predicate predicate = query.getPredicate();

        if (predicate != null) {
            Condition condition = createWhereCondition(predicate, null, false);

            if (condition != null) {
                whereCondition = whereCondition.and(condition);
            }
        }

        String extraWhere = ObjectUtils.to(String.class, query.getOptions().get(SqlDatabase.EXTRA_WHERE_QUERY_OPTION));

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
        HashMap<String, String> joinTableAliases = new HashMap<>();

        for (Join join : joins) {

            if (join.indexKeys.isEmpty()) {
                continue;
            }

            for (String indexKey : join.indexKeys) {
                joinTableAliases.put(join.getTableName().toLowerCase(Locale.ENGLISH) + join.quoteIndexKey(indexKey), join.getAlias());
            }

            // e.g. JOIN RecordIndex AS i#
            fromBuilder.append('\n');
            fromBuilder.append((forceLeftJoins ? JoinType.LEFT_OUTER : join.type).token);
            fromBuilder.append(' ');
            fromBuilder.append(dslContext.renderContext().declareTables(true).render(join.table));

            if (join.type == JoinType.INNER && join.equals(mysqlIndexHint)) {
                fromBuilder.append(" /*! USE INDEX (k_name_value) */");

            } else if (join.sqlIndex == SqlIndex.LOCATION
                    && join.sqlIndexTable.getVersion() >= 2) {
                fromBuilder.append(" /*! IGNORE INDEX (PRIMARY) */");
            }

            if ((join.sqlIndex == SqlIndex.LOCATION && join.sqlIndexTable.getVersion() < 3)
                    || (join.sqlIndex == SqlIndex.NUMBER && join.sqlIndexTable.getVersion() < 3)
                    || (join.sqlIndex == SqlIndex.STRING && join.sqlIndexTable.getVersion() < 4)
                    || (join.sqlIndex == SqlIndex.UUID && join.sqlIndexTable.getVersion() < 3)) {
                mysqlIgnoreIndexPrimaryDisabled = true;
            }

            // e.g. ON i#.recordId = r.id
            fromBuilder.append(" ON ");

            Condition joinCondition = join.idField.eq(recordIdField);

            // AND i#.typeId = r.typeId
            if (join.typeIdField != null) {
                joinCondition = joinCondition.and(join.typeIdField.eq(recordTypeIdField));
            }

            // AND i#.symbolId in (...)
            joinCondition = joinCondition.and(
                    join.keyField.in(
                            join.indexKeys.stream()
                                    .map(join::convertIndexKey)
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

        this.whereCondition = whereCondition;

        StringBuilder havingBuilder = new StringBuilder();
        String extraHaving = ObjectUtils.to(String.class, query.getOptions().get(SqlDatabase.EXTRA_HAVING_QUERY_OPTION));
        havingBuilder.append(ObjectUtils.isBlank(extraHaving) ? "" : ("\n" + (ObjectUtils.isBlank(this.havingClause) ? "HAVING" : "AND") + " " + extraHaving));
        this.havingClause = havingBuilder.toString();

        this.orderByClause = orderByBuilder.toString();
        this.fromClause = fromBuilder.toString();

    }

    // Creates jOOQ Condition from Dari Predicate.
    private Condition createWhereCondition(
            Predicate predicate,
            Predicate parentPredicate,
            boolean usesLeftJoin) {

        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            String operator = compoundPredicate.getOperator();
            boolean isNot = PredicateParser.NOT_OPERATOR.equals(operator);

            // e.g. (child1) OR (child2) OR ... (child#)
            if (isNot || PredicateParser.OR_OPERATOR.equals(operator)) {
                List<Predicate> children = compoundPredicate.getChildren();
                boolean usesLeftJoinChildren;

                if (children.size() > 1) {
                    usesLeftJoinChildren = true;
                    needsDistinct = true;

                } else {
                    usesLeftJoinChildren = isNot;
                }

                Condition compoundCondition = null;

                for (Predicate child : children) {
                    Condition childCondition = createWhereCondition(child, predicate, usesLeftJoinChildren);

                    if (childCondition != null) {
                        compoundCondition = compoundCondition != null
                                ? compoundCondition.or(childCondition)
                                : childCondition;
                    }
                }

                return isNot && compoundCondition != null
                        ? compoundCondition.not()
                        : compoundCondition;

            // e.g. (child1) AND (child2) AND .... (child#)
            } else if (PredicateParser.AND_OPERATOR.equals(operator)) {
                Condition compoundCondition = null;

                for (Predicate child : compoundPredicate.getChildren()) {
                    Condition childCondition = createWhereCondition(child, predicate, usesLeftJoin);

                    if (childCondition != null) {
                        compoundCondition = compoundCondition != null
                                ? compoundCondition.and(childCondition)
                                : childCondition;
                    }
                }

                return compoundCondition;
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
                            && j.sqlIndex.equals(SqlIndex.Static.getByType(mappedKeys.get(queryKey).getInternalType()))) {

                        needsDistinct = true;
                        join = j;

                        join.addIndexKey(queryKey);
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

            if (isFieldCollection
                    && (join.sqlIndexTable == null
                    || join.sqlIndexTable.getVersion() < 2)) {

                needsDistinct = true;
            }

            Field<Object> joinValueField = join.getValueField(queryKey, comparisonPredicate);
            Query<?> valueQuery = mappedKey.getSubQueryWithComparison(comparisonPredicate);
            String operator = comparisonPredicate.getOperator();
            boolean isNotEqualsAll = PredicateParser.NOT_EQUALS_ALL_OPERATOR.equals(operator);

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

                    return subQueryCondition;

                } else {
                    SqlQuery subSqlQuery = getOrCreateSubSqlQuery(valueQuery, join.type == JoinType.LEFT_OUTER);

                    subQueries.put(valueQuery, renderContext.render(joinValueField) + (isNotEqualsAll ? " != " : " = "));
                    return subSqlQuery.whereCondition;
                }
            }

            List<Condition> comparisonConditions = new ArrayList<>();
            boolean hasMissing = false;
            int subClauseCount = 0;

            if (isNotEqualsAll || PredicateParser.EQUALS_ANY_OPERATOR.equals(operator)) {
                for (Object value : comparisonPredicate.resolveValues(database)) {
                    if (value == null) {
                        ++ subClauseCount;

                        comparisonConditions.add(DSL.falseCondition());

                    } else if (value == Query.MISSING_VALUE) {
                        ++ subClauseCount;
                        hasMissing = true;

                        if (isNotEqualsAll) {
                            if (isFieldCollection) {
                                needsDistinct = true;
                            }

                            comparisonConditions.add(joinValueField.isNotNull());

                        } else {
                            join.type = JoinType.LEFT_OUTER;

                            comparisonConditions.add(joinValueField.isNull());
                        }

                    } else if (value instanceof Region) {
                        List<Location> locations = ((Region) value).getLocations();

                        if (!locations.isEmpty()) {
                            ++ subClauseCount;

                            try {
                                StringBuilder rcb = new StringBuilder();

                                vendor.appendWhereRegion(rcb, (Region) value, renderContext.render(joinValueField));

                                Condition rc = DSL.condition(rcb.toString());

                                if (isNotEqualsAll) {
                                    rc = rc.not();
                                }

                                comparisonConditions.add(rc);

                            } catch (UnsupportedIndexException uie) {
                                throw new UnsupportedIndexException(vendor, queryKey);
                            }
                        }

                    } else {
                        Object convertedValue = join.convertValue(comparisonPredicate, value);

                        ++ subClauseCount;

                        if (isNotEqualsAll) {
                            join.type = JoinType.LEFT_OUTER;
                            needsDistinct = true;
                            hasMissing = true;

                            comparisonConditions.add(
                                    joinValueField.isNull().or(
                                            joinValueField.ne(convertedValue)));

                        } else {
                            comparisonConditions.add(joinValueField.eq(convertedValue));
                        }
                    }
                }

            } else {
                SqlQueryComparison sqlQueryComparison = SqlQueryComparison.find(operator);

                // e.g. field OP value1 OR field OP value2 OR ... field OP value#
                if (sqlQueryComparison != null) {
                    for (Object value : comparisonPredicate.resolveValues(database)) {
                        ++ subClauseCount;

                        if (value == null) {
                            comparisonConditions.add(DSL.falseCondition());

                        } else if (value instanceof Location) {
                            ++ subClauseCount;

                            try {
                                StringBuilder lb = new StringBuilder();

                                vendor.appendWhereLocation(lb, (Location) value, renderContext.render(joinValueField));
                                comparisonConditions.add(DSL.condition(lb.toString()));

                            } catch (UnsupportedIndexException uie) {
                                throw new UnsupportedIndexException(vendor, queryKey);
                            }

                        } else if (value == Query.MISSING_VALUE) {
                            hasMissing = true;
                            join.type = JoinType.LEFT_OUTER;

                            comparisonConditions.add(joinValueField.isNull());

                        } else {
                            comparisonConditions.add(
                                    sqlQueryComparison.createCondition(
                                            joinValueField,
                                            join.convertValue(comparisonPredicate, value)));
                        }
                    }
                }
            }

            if (comparisonConditions.isEmpty()) {
                return isNotEqualsAll ? DSL.trueCondition() : DSL.falseCondition();
            }

            Condition whereCondition = isNotEqualsAll
                    ? DSL.and(comparisonConditions)
                    : DSL.or(comparisonConditions);

            if (!hasMissing) {
                if (join.needsIndexTable) {
                    String indexKey = mappedKeys.get(queryKey).getIndexKey(selectedIndexes.get(queryKey));
                    if (indexKey != null) {
                        whereCondition = join.keyField.eq(join.convertIndexKey(indexKey)).and(whereCondition);
                    }
                }

                if (join.needsIsNotNull) {
                    whereCondition = joinValueField.isNotNull().and(whereCondition);
                }

                if (subClauseCount > 1) {
                    needsDistinct = true;
                }
            }

            return whereCondition;
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
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));
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
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));
        statementBuilder.append(havingClause);
        statementBuilder.append(orderByClause);

        return statementBuilder.toString();
    }

    /**
     * Returns an SQL statement that can be used to get all objects
     * grouped by the values of the given {@code groupFields}.
     */
    public String groupStatement(String[] groupFields) {
        Map<String, Join> groupJoins = new CompactMap<>();
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

        statementBuilder.append("\nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');
        statementBuilder.append(fromClause.replace(" /*! USE INDEX (k_name_value) */", ""));
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));

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

        statementBuilder.append(groupBy.toString());

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
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));

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

        String extraColumns = ObjectUtils.to(String.class, query.getOptions().get(SqlDatabase.EXTRA_COLUMNS_QUERY_OPTION));

        if (extraColumns != null) {
            statementBuilder.append(", ");
            statementBuilder.append(extraColumns);
        }

        if (!needsDistinct && !subSqlQueries.isEmpty()) {
            for (Map.Entry<Query<?>, SqlQuery> entry : subSqlQueries.entrySet()) {
                SqlQuery subSqlQuery = entry.getValue();
                statementBuilder.append(", " + subSqlQuery.aliasPrefix + "r." + SqlDatabase.ID_COLUMN + " AS " + SqlDatabase.SUB_DATA_COLUMN_ALIAS_PREFIX + subSqlQuery.aliasPrefix + "_" + SqlDatabase.ID_COLUMN);
                statementBuilder.append(", " + subSqlQuery.aliasPrefix + "r." + SqlDatabase.TYPE_ID_COLUMN + " AS " + SqlDatabase.SUB_DATA_COLUMN_ALIAS_PREFIX + subSqlQuery.aliasPrefix + "_" + SqlDatabase.TYPE_ID_COLUMN);
                statementBuilder.append(", " + subSqlQuery.aliasPrefix + "r." + SqlDatabase.DATA_COLUMN + " AS " + SqlDatabase.SUB_DATA_COLUMN_ALIAS_PREFIX + subSqlQuery.aliasPrefix + "_" + SqlDatabase.DATA_COLUMN);
            }
        }

        statementBuilder.append("\nFROM ");
        vendor.appendIdentifier(statementBuilder, "Record");
        statementBuilder.append(' ');
        statementBuilder.append(aliasPrefix);
        statementBuilder.append('r');

        if (fromClause.length() > 0
                && !fromClause.contains("LEFT OUTER JOIN")
                && !mysqlIgnoreIndexPrimaryDisabled) {
            statementBuilder.append(" /*! IGNORE INDEX (PRIMARY) */");
        }

        statementBuilder.append(fromClause);
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));
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

            if (!query.getExtraSourceColumns().isEmpty()) {
                for (String extraSourceColumn : query.getExtraSourceColumns().keySet()) {
                    distinctBuilder.append(", ");
                    vendor.appendIdentifier(distinctBuilder, "d0");
                    distinctBuilder.append('.');
                    vendor.appendIdentifier(distinctBuilder, extraSourceColumn);
                }
            }

            distinctBuilder.append(" FROM ");
            vendor.appendIdentifier(distinctBuilder, SqlDatabase.RECORD_TABLE);
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
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));
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
        if (queryKey.equals(query.getOptions().get(SqlDatabase.MYSQL_INDEX_HINT_QUERY_OPTION))) {
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
        private final SqlIndex sqlIndex;
        private final SqlIndex.Table sqlIndexTable;
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
            this.sqlIndex = this.index != null
                    ? SqlIndex.Static.getByIndex(this.index)
                    : SqlIndex.Static.getByType(this.indexType);

            if (Query.ID_KEY.equals(queryKey)) {
                needsIndexTable = false;
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
                addIndexKey(queryKey);
                valueField = null;
                sqlIndexTable = this.sqlIndex.getReadTable(database, index);

                tableName = sqlIndexTable.getName(database, index);
                table = DSL.table(DSL.name(tableName)).as(aliasPrefix + alias);

                idField = aliasedField(alias, sqlIndexTable.getIdField(database, index));
                typeIdField = aliasedField(alias, sqlIndexTable.getTypeIdField(database, index));
                keyField = aliasedField(alias, sqlIndexTable.getKeyField(database, index));
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

        public Object convertIndexKey(String indexKey) {
            return sqlIndexTable.convertKey(database, index, indexKey);
        }

        public Object quoteIndexKey(String indexKey) {
            return SqlDatabase.quoteValue(convertIndexKey(indexKey));
        }

        public Object convertValue(ComparisonPredicate comparison, Object value) {
            Query.MappedKey mappedKey = mappedKeys.get(comparison.getKey());
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

            } else if (sqlIndex != SqlIndex.CUSTOM) {
                field = aliasedField(alias, sqlIndexTable.getValueField(database, index, 0));

            } else {
                String valueFieldName = mappedKeys.get(queryKey).getField().getInternalName();
                List<String> fieldNames = index.getFields();
                int fieldIndex = 0;
                for (int size = fieldNames.size(); fieldIndex < size; ++ fieldIndex) {
                    if (valueFieldName.equals(fieldNames.get(fieldIndex))) {
                        break;
                    }
                }
                field = aliasedField(alias, sqlIndexTable.getValueField(database, index, fieldIndex));
            }

            if (comparison != null
                    && comparison.isIgnoreCase()
                    && (sqlIndex != SqlIndex.STRING
                    || sqlIndexTable.getVersion() < 3)) {

                field = DSL.field(vendor.convertRawToStringSql(renderContext.render(field)), String.class).lower();
            }

            return (Field<Object>) field;
        }
    }
}
