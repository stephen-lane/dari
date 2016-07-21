package com.psddev.dari.db.h2;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import com.psddev.dari.util.TypeDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public abstract class AbstractIndexTest<T> extends AbstractTest {

    protected int total;

    protected abstract Class<? extends Model<T>> modelClass();

    protected abstract T value(int index);

    protected ModelBuilder model() {
        return new ModelBuilder();
    }

    @Before
    public void resetTotal() {
        total = 0;
    }

    protected Query<? extends Model<T>> query() {
        return Query.from(modelClass());
    }

    @After
    public void deleteModels() {
        query().deleteAll();
    }

    @Test
    public void invalidValue() {
        Model<T> model = TypeDefinition.getInstance(modelClass()).newInstance();
        model.getState().put("field", new Object());
        model.save();

        assertThat(
                Query.from(modelClass()).first().field,
                nullValue());
    }

    protected void assertCount(long count, String predicate, String notPredicate, Object... parameters) {
        assertThat(predicate, query().where(predicate, parameters).count(), is(count));
        assertThat(notPredicate, query().where(notPredicate, parameters).count(), is(total - count));
    }

    protected void createMissingTestModels() {
        model().create();
        model().field(0).create();
        model().set(0).create();
        model().list(0).create();
        model().field(0).set(0).create();
        model().field(0).list(0).create();
        model().set(0).list(0).create();
        model().all(0).create();
    }

    protected void missing(String field, long count) {
        assertCount(count,
                field + " = missing",
                field + " != missing");
    }

    @Test
    public void missing() {
        createMissingTestModels();
        missing("field", 4L);
        missing("set", 4L);
        missing("list", 4L);
    }

    protected void missingBoth(String field1, String field2, long count) {
        assertCount(count,
                field1 + " = missing and " + field2 + " = missing",
                field1 + " != missing or " + field2 + " != missing");
    }

    @Test
    public void missingBoth() {
        createMissingTestModels();
        missingBoth("field", "set", 2L);
        missingBoth("field", "list", 2L);
        missingBoth("set", "list", 2L);
    }

    protected void missingEither(String field1, String field2, long count) {
        assertCount(count,
                field1 + " = missing or " + field2 + " = missing",
                field1 + " != missing and " + field2 + " != missing");
    }

    @Test
    public void missingEither() {
        createMissingTestModels();
        missingEither("field", "set", 6L);
        missingEither("field", "list", 6L);
        missingEither("set", "list", 6L);
    }

    protected void createCompareTestModels() {
        IntStream.range(0, 5).forEach(i -> model().all(i).create());
    }

    protected void compare(String field, String operator, String notOperator, int index, long count) {
        assertCount(count,
                field + " " + operator + " ?",
                field + " " + notOperator + " ?",
                value(index));
    }

    @Test
    public void eq() {
        createCompareTestModels();
        compare("field", "=", "!=", 2, 1L);
        compare("set", "=", "!=", 2, 1L);
        compare("list", "=", "!=", 2, 1L);
    }

    @Test
    public void gt() {
        createCompareTestModels();
        compare("field", ">", "<=", 2, 2L);
    }

    @Test
    public void lt() {
        createCompareTestModels();
        compare("field", "<", ">=", 2, 2L);
    }

    public static class Model<T> extends Record {

        @Indexed
        public T field;

        @Indexed
        public final Set<T> set = new LinkedHashSet<>();

        @Indexed
        public final List<T> list = new ArrayList<>();
    }

    protected class ModelBuilder {

        private final Model<T> model;

        public ModelBuilder() {
            model = TypeDefinition.getInstance(modelClass()).newInstance();
        }

        public ModelBuilder all(int index) {
            T value = value(index);
            model.field = value;
            model.set.add(value);
            model.list.add(value);
            model.list.add(value);
            return this;
        }

        public ModelBuilder field(int index) {
            model.field = value(index);
            return this;
        }

        public ModelBuilder set(int index) {
            model.set.add(value(index));
            return this;
        }

        public ModelBuilder list(int index) {
            T value = value(index);
            model.list.add(value);
            model.list.add(value);
            return this;
        }

        public void create() {
            model.save();
            ++ total;
        }
    }
}
