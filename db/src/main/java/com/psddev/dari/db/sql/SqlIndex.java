package com.psddev.dari.db.sql;

import java.net.URI;
import java.net.URL;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.ObjectMethod;
import com.psddev.dari.db.ObjectStruct;
import com.psddev.dari.db.ObjectType;
import com.psddev.dari.db.Recordable;
import com.psddev.dari.db.State;
import com.psddev.dari.util.ObjectToIterable;
import com.psddev.dari.util.StringUtils;
import org.jooq.DSLContext;

/** Internal representations of all SQL index tables. */
enum SqlIndex {

    LOCATION(
        new SqlIndexTable.NameSingleValue(1, "RecordLocation"),
        new SqlIndexTable.SymbolIdSingleValue(2, "RecordLocation2"),
        new SqlIndexTable.TypeIdSymbolIdSingleValue(3, "RecordLocation3")
    ),

    REGION(
        new SqlIndexTable.SymbolIdSingleValue(1, "RecordRegion"),
        new SqlIndexTable.TypeIdSymbolIdSingleValue(2, "RecordRegion2")
    ),

    NUMBER(
        new SqlIndexTable.NameSingleValue(1, "RecordNumber"),
        new SqlIndexTable.SymbolIdSingleValue(2, "RecordNumber2"),
        new SqlIndexTable.TypeIdSymbolIdSingleValue(3, "RecordNumber3")
    ),

    STRING(
        new SqlIndexTable.NameSingleValue(1, "RecordString") {
            @Override
            protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                String string = value.toString();
                return string.length() > 400 ? string.substring(0, 400) : string;
            }
        },

        new SqlIndexTable.SymbolIdSingleValue(2, "RecordString2") {
            @Override
            protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                return stringToBytes(value.toString(), 500);
            }
        },

        new SqlIndexTable.SymbolIdSingleValue(3, "RecordString3") {
            @Override
            protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                String valueString = value.toString().trim();
                if (!index.isCaseSensitive()) {
                    valueString = valueString.toLowerCase(Locale.ENGLISH);
                }
                return stringToBytes(valueString, 500);
            }
        },

        new SqlIndexTable.TypeIdSymbolIdSingleValue(4, "RecordString4") {
            @Override
            protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
                String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());
                if (!index.isCaseSensitive()) {
                    valueString = valueString.toLowerCase(Locale.ENGLISH);
                }
                return stringToBytes(valueString, 500);
            }
        }
    ),

    UUID(
        new SqlIndexTable.NameSingleValue(1, "RecordUuid"),
        new SqlIndexTable.SymbolIdSingleValue(2, "RecordUuid2"),
        new SqlIndexTable.TypeIdSymbolIdSingleValue(3, "RecordUuid3")
    );

    private final SqlIndexTable[] tables;

    SqlIndex(SqlIndexTable... tables) {
        this.tables = tables;
    }

    /**
     * Returns the instance that should be used to index values
     * of the given field {@code type}.
     */
    public static SqlIndex getByType(String type) {
        if (ObjectField.DATE_TYPE.equals(type)
                || ObjectField.NUMBER_TYPE.equals(type)) {
            return SqlIndex.NUMBER;

        } else if (ObjectField.LOCATION_TYPE.equals(type)) {
            return SqlIndex.LOCATION;

        } else if (ObjectField.REGION_TYPE.equals(type)) {
            return SqlIndex.REGION;

        } else if (ObjectField.RECORD_TYPE.equals(type)
                || ObjectField.UUID_TYPE.equals(type)) {
            return SqlIndex.UUID;

        } else {
            return SqlIndex.STRING;
        }
    }

    /**
     * Returns the instance that should be used to index values
     * of the given {@code index}.
     */
    public static SqlIndex getByIndex(ObjectIndex index) {
        List<String> fieldNames = index.getFields();
        ObjectField field = index.getParent().getField(fieldNames.get(0));
        String type = field != null ? field.getInternalItemType() : index.getType();

        return getByType(type);
    }

    /**
     * Deletes all index rows associated with the given {@code states}.
     */
    public static void deleteByStates(
            AbstractSqlDatabase database,
            SqlSchema schema,
            Connection connection,
            DSLContext context,
            List<State> states)
            throws SQLException {

        deleteByStates(database, schema, connection, context, null, states);
    }

    private static void deleteByStates(
            AbstractSqlDatabase database,
            SqlSchema schema,
            Connection connection,
            DSLContext context,
            ObjectIndex onlyIndex,
            List<State> states)
            throws SQLException {

        if (states == null || states.isEmpty()) {
            return;
        }

        SqlVendor vendor = database.getVendor();
        StringBuilder idsBuilder = new StringBuilder(" IN (");

        for (State state : states) {
            ObjectType type = state.getType();

            vendor.appendUuid(idsBuilder, state.getId());
            idsBuilder.append(",");
        }

        idsBuilder.setCharAt(idsBuilder.length() - 1, ')');

        for (SqlIndex sqlIndex : SqlIndex.values()) {
            for (SqlIndexTable table : sqlIndex.getWriteTables(database, null)) {
                StringBuilder deleteBuilder = new StringBuilder();
                deleteBuilder.append("DELETE FROM ");
                vendor.appendIdentifier(deleteBuilder, table.getName(database, onlyIndex));
                deleteBuilder.append(" WHERE ");
                vendor.appendIdentifier(deleteBuilder, table.getIdField(database, onlyIndex));
                deleteBuilder.append(idsBuilder);
                if (onlyIndex != null && table.getKeyField(database, onlyIndex) != null) {
                    deleteBuilder.append(" AND ");
                    vendor.appendIdentifier(deleteBuilder, table.getKeyField(database, onlyIndex));
                    deleteBuilder.append(" = ");
                    deleteBuilder.append(database.getReadSymbolId(onlyIndex.getUniqueName()));
                }
                AbstractSqlDatabase.Static.executeUpdateWithArray(vendor, connection, deleteBuilder.toString());
            }
        }
    }

    public static void updateByStates(
            AbstractSqlDatabase database,
            SqlSchema schema,
            Connection connection,
            DSLContext context,
            ObjectIndex index,
            List<State> states)
            throws SQLException {

        Map<String, String> updateQueries = new HashMap<>();
        Map<String, List<List<Object>>> updateParameters = new HashMap<>();
        Map<String, Set<String>> updateBindKeys = new HashMap<>();
        Map<String, List<State>> updateStates = new HashMap<>();
        Set<State> needDeletes = new HashSet<>();
        Set<State> needInserts = new HashSet<>();

        for (State state : states) {
            UUID id = state.getId();
            UUID typeId = state.getVisibilityAwareTypeId();

            List<SqlIndexValue> indexValues = new ArrayList<>();
            Map<String, Object> stateValues = state.getValues();
            collectIndexValues(state, indexValues, null, state.getDatabase().getEnvironment(), stateValues, index);
            ObjectType type = state.getType();
            if (type != null) {
                ObjectField field = type.getField(index.getField());
                if (field != null && field.isInternalCollectionType()) {
                    needInserts.add(state);
                    continue;
                }
                collectIndexValues(state, indexValues, null, type, stateValues, index);
            }

            for (SqlIndexValue indexValue : indexValues) {
                for (SqlIndexTable table : getByIndex(index).getWriteTables(database, index)) {

                    String name = table.getName(database, index);
                    String sqlQuery = updateQueries.get(name);
                    List<List<Object>> parameters = updateParameters.get(name);
                    Set<String> bindKeys = updateBindKeys.get(name);
                    List<State> tableStates = updateStates.get(name);
                    if (sqlQuery == null && parameters == null && tableStates == null) {
                        sqlQuery = table.prepareUpdateStatement(database, connection, index);
                        updateQueries.put(name, sqlQuery);

                        parameters = new ArrayList<>();
                        updateParameters.put(name, parameters);

                        bindKeys = new HashSet<>();
                        updateBindKeys.put(name, bindKeys);

                        tableStates = new ArrayList<>();
                        updateStates.put(name, tableStates);
                    }

                    table.bindUpdateValues(database, index, id, typeId, indexValue, bindKeys, parameters);
                    tableStates.add(state);
                }
            }
            if (indexValues.isEmpty()) {
                needDeletes.add(state);
            }

        }

        for (Map.Entry<String, String> entry : updateQueries.entrySet()) {
            String name = entry.getKey();
            String sqlQuery = entry.getValue();
            List<List<Object>> parameters = updateParameters.get(name);
            List<State> tableStates = updateStates.get(name);
            try {
                if (!parameters.isEmpty()) {
                    int[] rows = AbstractSqlDatabase.Static.executeBatchUpdate(connection, sqlQuery, parameters);
                    for (int i = 0; i < rows.length; i++) {
                        if (rows[i] == 0) {
                            needInserts.add(tableStates.get(i));
                        }
                    }
                }
            } catch (BatchUpdateException bue) {
                AbstractSqlDatabase.Static.logBatchUpdateException(bue, sqlQuery, parameters);
                throw bue;
            }
        }
        if (!needDeletes.isEmpty()) {
            deleteByStates(database, schema, connection, context, index, new ArrayList<>(needDeletes));
        }
        if (!needInserts.isEmpty()) {
            List<State> insertStates = new ArrayList<>(needInserts);
            deleteByStates(database, schema, connection, context, index, insertStates);
            insertByStates(database, schema, connection, context, index, insertStates);
        }
    }

    /**
     * Inserts all index rows associated with the given {@code states}.
     */
    public static void insertByStates(
            AbstractSqlDatabase database,
            SqlSchema schema,
            Connection connection,
            DSLContext context,
            List<State> states)
            throws SQLException {

        insertByStates(database, schema, connection, context, null, states);
    }

    private static void insertByStates(
            AbstractSqlDatabase database,
            SqlSchema schema,
            Connection connection,
            DSLContext context,
            ObjectIndex onlyIndex,
            List<State> states)
            throws SQLException {

        Map<String, String> insertQueries = new HashMap<>();
        Map<String, List<List<Object>>> insertParameters = new HashMap<>();
        Map<String, Set<String>> insertBindKeys = new HashMap<>();

        for (State state : states) {
            UUID id = state.getId();
            UUID typeId = state.getVisibilityAwareTypeId();

            for (SqlIndexValue indexValue : getIndexValues(state)) {
                ObjectIndex index = indexValue.getIndex();
                if (onlyIndex != null && !onlyIndex.equals(index)) {
                    continue;
                }

                for (SqlIndexTable table : getByIndex(index).getWriteTables(database, index)) {
                    String name = table.getName(database, index);
                    String sqlQuery = insertQueries.get(name);
                    List<List<Object>> parameters = insertParameters.get(name);
                    Set<String> bindKeys = insertBindKeys.get(name);
                    if (sqlQuery == null && parameters == null) {
                        sqlQuery = table.prepareInsertStatement(database, connection, index);
                        insertQueries.put(name, sqlQuery);

                        parameters = new ArrayList<>();
                        insertParameters.put(name, parameters);

                        bindKeys = new HashSet<>();
                        insertBindKeys.put(name, bindKeys);
                    }

                    table.bindInsertValues(database, index, id, typeId, indexValue, bindKeys, parameters);
                }
            }
        }

        for (Map.Entry<String, String> entry : insertQueries.entrySet()) {
            String name = entry.getKey();
            String sqlQuery = entry.getValue();
            List<List<Object>> parameters = insertParameters.get(name);
            try {
                if (!parameters.isEmpty()) {
                    AbstractSqlDatabase.Static.executeBatchUpdate(connection, sqlQuery, parameters);
                }
            } catch (BatchUpdateException bue) {
                AbstractSqlDatabase.Static.logBatchUpdateException(bue, sqlQuery, parameters);
                throw bue;
            }
        }
    }

    /**
     * Returns a list of indexable values in this state. This is a helper
     * method for database implementations and isn't meant for general
     * consumption.
     */
    public static List<SqlIndexValue> getIndexValues(State state) {
        List<SqlIndexValue> indexValues = new ArrayList<>();
        Map<String, Object> values = state.getValues();

        collectIndexValues(state, indexValues, null, state.getDatabase().getEnvironment(), values);

        ObjectType type = state.getType();
        if (type != null) {
            collectIndexValues(state, indexValues, null, type, values);
        }

        return indexValues;
    }

    private static void collectIndexValues(
            State state,
            List<SqlIndexValue> indexValues,
            ObjectField[] prefixes,
            ObjectStruct struct,
            Map<String, Object> stateValues,
            ObjectIndex index) {

        List<Set<Object>> valuesList = new ArrayList<>();

        for (String fieldName : index.getFields()) {
            ObjectField field = struct.getField(fieldName);
            if (field == null) {
                return;
            }

            Set<Object> values = new HashSet<>();
            Object fieldValue;
            if (field instanceof ObjectMethod) {
                StringBuilder path = new StringBuilder();
                if (prefixes != null) {
                    for (ObjectField fieldPrefix : prefixes) {
                        path.append(fieldPrefix.getInternalName());
                        path.append("/");
                    }
                }
                path.append(field.getInternalName());
                fieldValue = state.getByPath(path.toString());
            } else {
                fieldValue = stateValues.get(field.getInternalName());
            }

            collectFieldValues(state, indexValues, prefixes, struct, field, values, fieldValue);
            if (values.isEmpty()) {
                return;
            }

            valuesList.add(values);
        }

        int valuesListSize = valuesList.size();
        int permutationSize = 1;
        for (Set<Object> values : valuesList) {
            permutationSize *= values.size();
        }

        // Calculate all permutations on multi-field indexes.
        Object[][] permutations = new Object[permutationSize][valuesListSize];
        int partitionSize = permutationSize;
        for (int i = 0; i < valuesListSize; ++ i) {
            Set<Object> values = valuesList.get(i);
            int valuesSize = values.size();
            partitionSize /= valuesSize;
            for (int p = 0; p < permutationSize;) {
                for (Object value : values) {
                    for (int k = 0; k < partitionSize; ++ k, ++ p) {
                        permutations[p][i] = value;
                    }
                }
            }
        }

        indexValues.add(new SqlIndexValue(prefixes, index, permutations));

    }

    private static void collectIndexValues(
            State state,
            List<SqlIndexValue> indexValues,
            ObjectField[] prefixes,
            ObjectStruct struct,
            Map<String, Object> stateValues) {

        for (ObjectIndex index : struct.getIndexes()) {
            collectIndexValues(state, indexValues, prefixes, struct, stateValues, index);
        }
    }

    private static void collectFieldValues(
            State state,
            List<SqlIndexValue> indexValues,
            ObjectField[] prefixes,
            ObjectStruct struct,
            ObjectField field,
            Set<Object> values,
            Object value) {

        if (value == null) {
            return;
        }

        Iterable<Object> valueIterable = ObjectToIterable.iterable(value);
        if (valueIterable != null) {
            for (Object item : valueIterable) {
                collectFieldValues(state, indexValues, prefixes, struct, field, values, item);
            }

        } else if (value instanceof Map) {
            for (Object item : ((Map<?, ?>) value).values()) {
                collectFieldValues(state, indexValues, prefixes, struct, field, values, item);
            }

        } else if (value instanceof Recordable) {
            State valueState = ((Recordable) value).getState();

            if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                ObjectType valueType = valueState.getType();

                if (field.isEmbedded()
                        || (valueType != null && valueType.isEmbedded())) {
                    int last;
                    ObjectField[] newPrefixes;

                    if (prefixes != null) {
                        last = prefixes.length;
                        newPrefixes = new ObjectField[last + 1];
                        System.arraycopy(prefixes, 0, newPrefixes, 0, last);

                    } else {
                        newPrefixes = new ObjectField[1];
                        last = 0;
                    }

                    newPrefixes[last] = field;
                    collectIndexValues(state, indexValues, newPrefixes, state.getDatabase().getEnvironment(), valueState.getValues());
                    collectIndexValues(state, indexValues, newPrefixes, valueType, valueState.getValues());

                } else {
                    values.add(valueState.getId());
                }

            } else {
                values.add(valueState.getId());
            }

        } else if (value instanceof Character
                || value instanceof CharSequence
                || value instanceof URI
                || value instanceof URL) {
            values.add(value.toString());

        } else if (value instanceof Date) {
            values.add(((Date) value).getTime());

        } else if (value instanceof Enum) {
            values.add(((Enum<?>) value).name());

        } else if (value instanceof Locale) {
            values.add(((Locale) value).toLanguageTag());

        } else {
            values.add(value);
        }
    }

    /**
     * Returns the table that can be used to read the values of the given
     * {@code index} from the given {@code database}.
     */
    public SqlIndexTable getReadTable(AbstractSqlDatabase database, ObjectIndex index) {
        for (SqlIndexTable table : tables) {
            if (database.hasTable(table.getName(database, index))) {
                return table;
            }
        }
        return tables[tables.length - 1];
    }

    /**
     * Returns all tables that should be written to when updating the
     * values of the index in the given {@code database}.
     */
    public List<SqlIndexTable> getWriteTables(AbstractSqlDatabase database, ObjectIndex index) {
        List<SqlIndexTable> writeTables = new ArrayList<>();

        if (!database.isIndexSpatial() && (LOCATION.equals(this) || REGION.equals(this))) {
            return writeTables;
        }

        for (SqlIndexTable table : tables) {
            if (database.hasTable(table.getName(database, index)) && !table.isReadOnly(index)) {
                writeTables.add(table);
            }
        }

        if (writeTables.isEmpty()) {
            SqlIndexTable lastTable = tables[tables.length - 1];

            if (!lastTable.isReadOnly(index)) {
                writeTables.add(lastTable);
            }
        }

        return writeTables;
    }
}
