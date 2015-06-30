package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.RenderContext;
import org.jooq.SQLDialect;
import org.jooq.SortField;
import org.jooq.SortOrder;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/** Internal representation of an SQL query based on a Dari one. */
class SqlQuery {

    private static final Pattern QUERY_KEY_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");
    //private static final Logger LOGGER = LoggerFactory.getLogger(SqlQuery.class);

    protected final SqlDatabase database;
    protected final Query<?> query;
    protected final String aliasPrefix;

    protected final SqlVendor vendor;
    private final DSLContext dslContext;
    private final RenderContext tableRenderContext;
    protected final RenderContext renderContext;
    private final Table<?> recordTable;
    protected final Field<UUID> recordIdField;
    protected final Field<UUID> recordTypeIdField;
    protected final Map<String, Query.MappedKey> mappedKeys;
    protected final Map<String, ObjectIndex> selectedIndexes;

    private String fromClause;
    private Condition whereCondition;
    private Condition havingCondition;
    private final List<SortField<?>> orderByFields = new ArrayList<>();
    private String orderByClause;
    protected final List<SqlQueryJoin> joins = new ArrayList<>();
    private final Map<Query<?>, String> subQueries = new CompactMap<>();
    private final Map<Query<?>, SqlQuery> subSqlQueries = new HashMap<>();

    private boolean needsDistinct;
    protected SqlQueryJoin mysqlIndexHint;
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
        tableRenderContext = dslContext.renderContext().paramType(ParamType.INLINED).declareTables(true);
        renderContext = dslContext.renderContext().paramType(ParamType.INLINED);

        String recordTableAlias = aliasPrefix + "r";

        recordTable = DSL.table(DSL.name("Record")).as(recordTableAlias);
        recordIdField = DSL.field(DSL.name(recordTableAlias, SqlDatabase.ID_COLUMN), vendor.uuidDataType());
        recordTypeIdField = DSL.field(DSL.name(recordTableAlias, SqlDatabase.TYPE_ID_COLUMN), vendor.uuidDataType());
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

    protected Field<Object> aliasedField(String alias, String field) {
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
                SqlQueryJoin join = SqlQueryJoin.findOrCreate(this, queryKey);
                join.type = JoinType.LEFT_OUTER_JOIN;
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

        // Creates jOOQ SortField from Dari Sorter.
        for (Sorter sorter : query.getSorters()) {
            String operator = sorter.getOperator();
            boolean ascending = Sorter.ASCENDING_OPERATOR.equals(operator);
            boolean descending = Sorter.DESCENDING_OPERATOR.equals(operator);
            boolean closest = Sorter.CLOSEST_OPERATOR.equals(operator);
            boolean farthest = Sorter.FARTHEST_OPERATOR.equals(operator);

            if (!(ascending || descending || closest || farthest)) {
                throw new UnsupportedSorterException(database, sorter);
            }

            String queryKey = (String) sorter.getOptions().get(0);
            SqlQueryJoin join = SqlQueryJoin.findOrCreateForSort(this, queryKey);
            Field<?> joinValueField = join.getValueField(queryKey, null);
            Query<?> subQuery = mappedKeys.get(queryKey).getSubQueryWithSorter(sorter, 0);

            if (subQuery != null) {
                SqlQuery subSqlQuery = getOrCreateSubSqlQuery(subQuery, true);

                subQueries.put(subQuery, renderContext.render(joinValueField) + " = ");
                orderByFields.addAll(subSqlQuery.orderByFields);
                continue;
            }

            if (ascending) {
                orderByFields.add(joinValueField.sort(SortOrder.ASC));

            } else if (descending) {
                orderByFields.add(joinValueField.sort(SortOrder.DESC));

            } else {
                try {
                    Location location = (Location) sorter.getOptions().get(1);
                    StringBuilder selectBuilder = new StringBuilder();
                    StringBuilder locationFieldBuilder = new StringBuilder();

                    vendor.appendNearestLocation(locationFieldBuilder, selectBuilder, location, renderContext.render(joinValueField));

                    Field<?> locationField = DSL.field(locationFieldBuilder.toString());

                    if (closest) {
                        orderByFields.add(locationField.sort(SortOrder.ASC));

                    } else {
                        orderByFields.add(locationField.sort(SortOrder.DESC));
                    }

                } catch (UnsupportedIndexException uie) {
                    throw new UnsupportedIndexException(vendor, queryKey);
                }
            }
        }

        StringBuilder orderByBuilder = new StringBuilder();

        if (!orderByFields.isEmpty()) {
            orderByBuilder.append(" ORDER BY ");

            for (SortField<?> orderByField : orderByFields) {
                orderByBuilder.append(renderContext.render(orderByField));
                orderByBuilder.append(", ");
            }

            orderByBuilder.setLength(orderByBuilder.length() - 2);
        }

        orderByClause = orderByBuilder.toString();

        // Builds the FROM clause.
        StringBuilder fromBuilder = new StringBuilder();

        for (SqlQueryJoin join : joins) {
            if (join.indexKeys.isEmpty()) {
                continue;
            }

            // e.g. JOIN RecordIndex AS i#
            fromBuilder.append('\n');
            fromBuilder.append((forceLeftJoins ? JoinType.LEFT_OUTER_JOIN : join.type).toSQL());
            fromBuilder.append(' ');
            fromBuilder.append(tableRenderContext.render(join.table));

            if (join.type == JoinType.JOIN && join.equals(mysqlIndexHint)) {
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

            String alias = subSqlQuery.aliasPrefix + "r";

            fromBuilder.append("\nINNER JOIN ");
            fromBuilder.append(tableRenderContext.render(DSL.table(DSL.name("Record")).as(alias)));
            fromBuilder.append(" ON ");
            fromBuilder.append(entry.getValue());
            fromBuilder.append(renderContext.render(DSL.field(DSL.name(alias, "id"))));
            fromBuilder.append(subSqlQuery.fromClause);
        }

        if (extraJoins != null) {
            fromBuilder.append(' ');
            fromBuilder.append(extraJoins);
        }

        this.whereCondition = whereCondition;

        String extraHaving = ObjectUtils.to(String.class, query.getOptions().get(SqlDatabase.EXTRA_HAVING_QUERY_OPTION));

        this.havingCondition = !ObjectUtils.isBlank(extraHaving)
                ? DSL.condition(extraHaving)
                : null;

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
            SqlQueryJoin join = null;

            if (mappedKey.getField() != null
                    && parentPredicate instanceof CompoundPredicate
                    && PredicateParser.OR_OPERATOR.equals(parentPredicate.getOperator())) {

                for (SqlQueryJoin j : joins) {
                    if (j.parent == parentPredicate
                            && j.sqlIndex.equals(SqlIndex.Static.getByType(mappedKeys.get(queryKey).getInternalType()))) {

                        needsDistinct = true;
                        join = j;

                        join.addIndexKey(queryKey);
                        break;
                    }
                }

                if (join == null) {
                    join = SqlQueryJoin.findOrCreate(this, queryKey);
                    join.parent = parentPredicate;
                }

            } else if (isFieldCollection) {
                join = SqlQueryJoin.create(this, queryKey);

            } else {
                join = SqlQueryJoin.findOrCreate(this, queryKey);
            }

            if (usesLeftJoin) {
                join.type = JoinType.LEFT_OUTER_JOIN;
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
                    SqlQuery subSqlQuery = getOrCreateSubSqlQuery(valueQuery, join.type == JoinType.LEFT_OUTER_JOIN);

                    subQueries.put(valueQuery, renderContext.render(joinValueField) + (isNotEqualsAll ? " != " : " = "));
                    return subSqlQuery.whereCondition;
                }
            }

            List<Condition> comparisonConditions = new ArrayList<>();
            boolean hasMissing = false;

            if (isNotEqualsAll || PredicateParser.EQUALS_ANY_OPERATOR.equals(operator)) {
                for (Object value : comparisonPredicate.resolveValues(database)) {
                    if (value == null) {
                        comparisonConditions.add(DSL.falseCondition());

                    } else if (value == Query.MISSING_VALUE) {
                        hasMissing = true;

                        if (isNotEqualsAll) {
                            if (isFieldCollection) {
                                needsDistinct = true;
                            }

                            comparisonConditions.add(joinValueField.isNotNull());

                        } else {
                            join.type = JoinType.LEFT_OUTER_JOIN;

                            comparisonConditions.add(joinValueField.isNull());
                        }

                    } else if (value instanceof Region) {
                        List<Location> locations = ((Region) value).getLocations();

                        if (!locations.isEmpty()) {
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

                        if (isNotEqualsAll) {
                            join.type = JoinType.LEFT_OUTER_JOIN;
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
                        if (value == null) {
                            comparisonConditions.add(DSL.falseCondition());

                        } else if (value instanceof Location) {
                            try {
                                StringBuilder lb = new StringBuilder();

                                vendor.appendWhereLocation(lb, (Location) value, renderContext.render(joinValueField));
                                comparisonConditions.add(DSL.condition(lb.toString()));

                            } catch (UnsupportedIndexException uie) {
                                throw new UnsupportedIndexException(vendor, queryKey);
                            }

                        } else if (value == Query.MISSING_VALUE) {
                            hasMissing = true;
                            join.type = JoinType.LEFT_OUTER_JOIN;

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

                if (comparisonConditions.size() > 1) {
                    needsDistinct = true;
                }
            }

            return whereCondition;
        }

        throw new UnsupportedPredicateException(this, predicate);
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
        initializeClauses();

        return renderContext.render(dslContext
                .select(needsDistinct ? recordIdField.countDistinct() : recordIdField.count())
                .from(DSL.table(tableRenderContext.render(recordTable)) + fromClause.replace(" /*! USE INDEX (k_name_value) */", ""))
                .where(whereCondition));
    }

    /**
     * Returns an SQL statement that can be used to delete all rows
     * matching the query.
     */
    public String deleteStatement() {
        initializeClauses();

        return renderContext.render(dslContext
                .deleteFrom(DSL.table(tableRenderContext.render(recordTable) + fromClause))
                .where(whereCondition));
    }

    /**
     * Returns an SQL statement that can be used to get all objects
     * grouped by the values of the given {@code groupFields}.
     */
    public String groupStatement(String[] groupFields) {
        Map<String, SqlQueryJoin> groupJoins = new CompactMap<>();
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
                SqlQueryJoin join = SqlQueryJoin.findOrCreate(this, groupField);
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
        for (Map.Entry<String, SqlQueryJoin> entry : groupJoins.entrySet()) {
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
        statementBuilder.append(tableRenderContext.render(recordTable));
        statementBuilder.append(fromClause.replace(" /*! USE INDEX (k_name_value) */", ""));
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));

        for (Map.Entry<String, SqlQueryJoin> entry : groupJoins.entrySet()) {
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

        if (havingCondition != null) {
            statementBuilder.append(" HAVING ");
            statementBuilder.append(renderContext.render(havingCondition));
        }

        statementBuilder.append(orderByClause);

        return statementBuilder.toString();
    }

    /**
     * Returns an SQL statement that can be used to get when the rows
     * matching the query were last updated.
     */
    public String lastUpdateStatement() {
        initializeClauses();

        String alias = aliasPrefix + "r";

        return renderContext.render(dslContext
                .select(DSL.field(DSL.name(alias, "updateDate")).max())
                .from(DSL.table(tableRenderContext.render(DSL.table("RecordUpdate").as(alias)) + fromClause))
                .where(whereCondition));
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
        statementBuilder.append(tableRenderContext.render(recordTable));

        if (fromClause.length() > 0
                && !fromClause.contains("LEFT OUTER JOIN")
                && !mysqlIgnoreIndexPrimaryDisabled) {
            statementBuilder.append(" /*! IGNORE INDEX (PRIMARY) */");
        }

        statementBuilder.append(fromClause);
        statementBuilder.append(" WHERE ");
        statementBuilder.append(renderContext.render(whereCondition));

        if (havingCondition != null) {
            statementBuilder.append(" HAVING ");
            statementBuilder.append(renderContext.render(havingCondition));
        }

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

    /**
     * Returns an SQL statement that can be used as a sub-query.
     */
    public String subQueryStatement() {
        initializeClauses();

        return renderContext.render((needsDistinct
                ? dslContext.selectDistinct(recordIdField)
                : dslContext.select(recordIdField))
                .from(DSL.table(tableRenderContext.render(recordTable) + fromClause))
                .where(whereCondition)
                .having(havingCondition)
                .orderBy(orderByFields));
    }
}
