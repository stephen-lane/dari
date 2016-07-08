package com.psddev.dari.db.sql;

import com.psddev.dari.db.ObjectField;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.ObjectType;

import java.util.Iterator;
import java.util.List;

class SqlIndexValue {

    private final ObjectField[] prefixes;
    private final ObjectIndex index;
    private final Object[][] valuesArray;

    public SqlIndexValue(ObjectField[] prefixes, ObjectIndex index, Object[][] valuesArray) {
        this.prefixes = prefixes;
        this.index = index;
        this.valuesArray = valuesArray;
    }

    public ObjectIndex getIndex() {
        return index;
    }

    public Object[][] getValuesArray() {
        return valuesArray;
    }

    /**
     * Returns a unique name that identifies this index value.
     * This is a helper method for database implementations and
     * isn't meant for general consumption.
     */
    public String getUniqueName() {
        StringBuilder nameBuilder = new StringBuilder();

        if (prefixes == null) {
            if (index.getParent() instanceof ObjectType) {
                nameBuilder.append(index.getJavaDeclaringClassName());
                nameBuilder.append('/');
            }

        } else {
            nameBuilder.append(prefixes[0].getUniqueName());
            nameBuilder.append('/');
            for (int i = 1, length = prefixes.length; i < length; ++i) {
                nameBuilder.append(prefixes[i].getInternalName());
                nameBuilder.append('/');
            }
        }

        Iterator<String> indexFieldsIterator = index.getFields().iterator();
        nameBuilder.append(indexFieldsIterator.next());
        while (indexFieldsIterator.hasNext()) {
            nameBuilder.append(',');
            nameBuilder.append(indexFieldsIterator.next());
        }

        return nameBuilder.toString();
    }

    public String getInternalType() {
        List<String> fields = index.getFields();
        return index.getParent().getField(fields.get(fields.size() - 1)).getInternalItemType();
    }
}
