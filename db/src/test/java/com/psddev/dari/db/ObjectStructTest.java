package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import com.google.common.base.Preconditions;
import java8.util.stream.Collectors;
import java8.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ObjectStructTest {

    @Test(expected = NullPointerException.class)
    public void findIndexedFieldsNull() {
        findIndexedFields(null);
    }

    @Test
    public void findIndexedFields() {
        List<ObjectField> fields = new ArrayList<>();

        for (int i = 0; i < 3; ++ i) {
            ObjectField field = mock(ObjectField.class);

            when(field.getInternalName()).thenReturn("field" + i);
            fields.add(field);
        }

        ObjectIndex index = mock(ObjectIndex.class);

        when(index.getUniqueName()).thenReturn("index0");

        ObjectField field0 = fields.get(0);
        ObjectField field1 = fields.get(1);

        when(index.getFields()).thenReturn(Arrays.asList("field0", "field1"));

        ObjectStruct struct = new TestObjectStruct();

        struct.setFields(fields);
        struct.setIndexes(Collections.singletonList(index));

        assertThat(
                findIndexedFields(struct),
                containsInAnyOrder(field0, field1));
    }

    private static class TestObjectStruct implements ObjectStruct {

        private DatabaseEnvironment environment = mock(DatabaseEnvironment.class);
        private List<ObjectField> fields;
        private List<ObjectIndex> indexes;

        @Override
        public DatabaseEnvironment getEnvironment() {
            return environment;
        }

        @Override
        public List<ObjectField> getFields() {
            return new ArrayList<>(fields);
        }

        @Override
        public ObjectField getField(String name) {
            return StreamSupport.stream(fields)
                    .filter(field -> field.getInternalName().equals(name))
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public void setFields(List<ObjectField> fields) {
            this.fields = fields != null ? fields : new ArrayList<>();
        }

        @Override
        public List<ObjectIndex> getIndexes() {
            return new ArrayList<>(indexes);
        }

        @Override
        public ObjectIndex getIndex(String name) {
            return StreamSupport.stream(indexes)
                    .filter(index -> index.getUniqueName().equals(name))
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public void setIndexes(List<ObjectIndex> indexes) {
            this.indexes = indexes != null ? indexes : new ArrayList<>();
        }
    }

    private List<ObjectField> findIndexedFields(ObjectStruct struct) {
        Preconditions.checkNotNull(struct);

        Set<String> indexed = StreamSupport.stream(struct.getIndexes())
                .flatMap(index -> StreamSupport.stream(index.getFields()))
                .collect(Collectors.toSet());

        List<ObjectField> fields = struct.getFields();

        fields.removeIf(field -> !indexed.contains(field.getInternalName()));

        return fields;
    }
}
