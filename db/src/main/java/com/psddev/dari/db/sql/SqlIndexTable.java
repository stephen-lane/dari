package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.ObjectStruct;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

class SqlIndexTable {

    private static final String VALUE_PARAM_NAME = "value";
    private static final Field<Object> VALUE_PARAM = DSL.param(VALUE_PARAM_NAME);

    private final int version;
    private final String name;
    private final String idField;
    private final String typeIdField;
    private final String keyField;
    private final String valueField;

    protected SqlIndexTable(int version, String name, String idField, String typeIdField, String keyField, String valueField) {
        this.version = version;
        this.name = name;
        this.idField = idField;
        this.typeIdField = typeIdField;
        this.keyField = keyField;
        this.valueField = valueField;
    }

    public SqlIndexTable(int version, String name) {
        this(version, name, "id", "typeId", "symbolId", "value");
    }

    public String getName(AbstractSqlDatabase database, ObjectIndex index) {
        return name;
    }

    public int getVersion() {
        return version;
    }

    public String getIdField(AbstractSqlDatabase database, ObjectIndex index) {
        return idField;
    }

    public String getTypeIdField(AbstractSqlDatabase database, ObjectIndex index) {
        return typeIdField;
    }

    public String getKeyField(AbstractSqlDatabase database, ObjectIndex index) {
        return keyField;
    }

    public String getValueField(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex) {
        return fieldIndex > 0 ? valueField + (fieldIndex + 1) : valueField;
    }

    public Field<?> valueParam(SqlSchema schema) {
        return VALUE_PARAM;
    }

    public Map<String, Object> createBindValues(AbstractSqlDatabase database, SqlSchema schema, ObjectIndex index, int fieldIndex, Object value) {
        value = convertValue(database, index, fieldIndex, value);

        if (ObjectUtils.isBlank(value)) {
            return null;
        }

        Map<String, Object> bindValues = new CompactMap<>();

        bindValues.put("value", value);

        return bindValues;
    }

    public Object convertReadKey(AbstractSqlDatabase database, ObjectIndex index, String key) {
        return database.getReadSymbolId(key);
    }

    public Object convertKey(AbstractSqlDatabase database, ObjectIndex index, String key) {
        return database.getSymbolId(key);
    }

    protected Object convertValue(AbstractSqlDatabase database, ObjectIndex index, int fieldIndex, Object value) {
        ObjectStruct parent = index.getParent();
        ObjectField field = parent.getField(index.getFields().get(fieldIndex));
        String type = field.getInternalItemType();

        if (ObjectField.DATE_TYPE.equals(type)
                || ObjectField.NUMBER_TYPE.equals(type)
                || ObjectField.LOCATION_TYPE.equals(type)
                || ObjectField.REGION_TYPE.equals(type)) {
            return value;

        } else if (value instanceof UUID) {
            return value;

        } else if (value instanceof String) {
            String valueString = StringUtils.trimAndCollapseWhitespaces(value.toString());
            if (!index.isCaseSensitive() && database.comparesIgnoreCase()) {
                valueString = valueString.toLowerCase(Locale.ENGLISH);
            }
            return stringToBytes(valueString, 500);

        } else {
            return value.toString();
        }
    }

    protected static byte[] stringToBytes(String value, int length) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        if (bytes.length <= length) {
            return bytes;

        } else {
            byte[] shortened = new byte[length];
            System.arraycopy(bytes, 0, shortened, 0, length);
            return shortened;
        }
    }

    public String prepareUpdateStatement(
            AbstractSqlDatabase database,
            Connection connection,
            ObjectIndex index) throws SQLException {

        SqlVendor vendor = database.getVendor();
        int fieldsSize = index.getFields().size();
        StringBuilder updateBuilder = new StringBuilder();

        updateBuilder.append("UPDATE ");
        vendor.appendIdentifier(updateBuilder, getName(database, index));
        updateBuilder.append(" SET ");

        for (int i = 0; i < fieldsSize; ++ i) {
            vendor.appendIdentifier(updateBuilder, getValueField(database, index, i));
            updateBuilder.append(" = ");
            if (SqlIndex.getByIndex(index) == SqlIndex.LOCATION) {
                vendor.appendBindLocation(updateBuilder, null, null);
            } else if (SqlIndex.getByIndex(index) == SqlIndex.REGION) {
                vendor.appendBindRegion(updateBuilder, null, null);
            } else {
                updateBuilder.append("?");
            }
            updateBuilder.append(", ");
        }
        if (fieldsSize > 0) {
            updateBuilder.setLength(updateBuilder.length() - 2);
        }

        updateBuilder.append(" WHERE ");
        vendor.appendIdentifier(updateBuilder, getIdField(database, index));
        updateBuilder.append(" = ?");

        if (getTypeIdField(database, index) != null) {
            updateBuilder.append(" AND ");
            vendor.appendIdentifier(updateBuilder, getTypeIdField(database, index));
            updateBuilder.append(" = ?");
        }

        updateBuilder.append(" AND ");
        vendor.appendIdentifier(updateBuilder, getKeyField(database, index));
        updateBuilder.append(" = ?");
        return updateBuilder.toString();
    }

    public void bindUpdateValues(
            AbstractSqlDatabase database,
            ObjectIndex index,
            UUID id,
            UUID typeId,
            SqlIndexValue indexValue,
            Set<String> bindKeys,
            List<List<Object>> parameters) throws SQLException {

        SqlVendor vendor = database.getVendor();
        Object indexKey = convertKey(database, index, indexValue.getUniqueName());
        int fieldsSize = index.getFields().size();
        StringBuilder updateBuilder = new StringBuilder();
        boolean writeIndex = true;

        for (Object[] valuesArray : indexValue.getValuesArray()) {
            StringBuilder bindKeyBuilder = new StringBuilder();
            bindKeyBuilder.append(id.toString());
            bindKeyBuilder.append(indexKey);

            for (int i = 0; i < fieldsSize; i++) {
                Object parameter = convertValue(database, index, i, valuesArray[i]);
                vendor.appendValue(bindKeyBuilder, parameter);

                if (ObjectUtils.isBlank(parameter)) {
                    writeIndex = false;
                    break;
                }
            }

            String bindKey = bindKeyBuilder.toString();

            if (writeIndex && !bindKeys.contains(bindKey)) {
                List<Object> rowData = new ArrayList<>();

                for (int i = 0; i < fieldsSize; i++) {
                    Object parameter = convertValue(database, index, i, valuesArray[i]);
                    vendor.appendBindValue(updateBuilder, parameter, rowData);
                }

                vendor.appendBindValue(updateBuilder, id, rowData);
                if (getTypeIdField(database, index) != null) {
                    vendor.appendBindValue(updateBuilder, typeId, rowData);
                }
                vendor.appendBindValue(updateBuilder, indexKey, rowData);

                bindKeys.add(bindKey);
                parameters.add(rowData);
            }
        }
    }
}
