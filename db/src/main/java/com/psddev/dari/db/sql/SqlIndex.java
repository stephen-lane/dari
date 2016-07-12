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
import org.jooq.DSLContext;

/** Internal representations of all SQL index tables. */
class SqlIndex {

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
                for (SqlIndexTable table : schema.findUpdateIndexTables(index)) {

                    String name = table.getName();
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
            schema.deleteIndexes(database, connection, context, index, new ArrayList<>(needDeletes));
        }
        if (!needInserts.isEmpty()) {
            List<State> insertStates = new ArrayList<>(needInserts);
            schema.deleteIndexes(database, connection, context, index, insertStates);
            schema.insertIndexes(database, connection, context, index, insertStates);
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
}
