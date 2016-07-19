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

    private long total;

    protected abstract Class<? extends Model<T>> modelClass();

    protected abstract T value(int index);

    private ModelBuilder model() {
        return new ModelBuilder();
    }

    @Before
    public void resetTotal() {
        total = 0L;
    }

    private Query<? extends Model<T>> query() {
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

    private void assertCount(long count, String predicate, String notPredicate, Object... parameters) {
        assertThat(predicate, query().where(predicate, parameters).count(), is(count));
        assertThat(notPredicate, query().where(notPredicate, parameters).count(), is(total - count));
    }

    private void missing(String field, long count) {
        assertCount(count,
                field + " = missing",
                field + " != missing");
    }

    @Test
    public void missing() {
        model().create();
        model().field(0).create();
        missing("field", 1L);
        missing("set", 1L);
        missing("list", 1L);
    }

    private void missingCompound(String operator, String field1, String field2, long count) {
        assertCount(count,
                field1 + " = missing " + operator + " " + field2 + " = missing",
                field1 + " != missing " + operator + " " + field2 + " != missing");
    }

    @Test
    public void missingBoth() {
        model().create();
        model().field(0).create();
        missingCompound("and", "field", "set", 1L);
        missingCompound("and", "field", "list", 1L);
        missingCompound("and", "set", "list", 1L);
    }

    @Test
    public void missingEither() {
    }

    private void compare(String field, String operator, String notOperator, int index, long count) {
        assertCount(count,
                field + " " + operator + " ?",
                field + " " + notOperator + " ?",
                value(index));
    }

    @Test
    public void eq() {
        IntStream.range(0, 5).forEach(i -> model().field(i).create());
        compare("field", "=", "!=", 2, 1L);
        compare("set", "=", "!=", 2, 1L);
        compare("list", "=", "!=", 2, 1L);
    }

    @Test
    public void gt() {
        IntStream.range(0, 5).forEach(i -> model().field(i).create());
        compare("field", ">", "<=", 2, 2L);
    }

    @Test
    public void lt() {
        IntStream.range(0, 5).forEach(i -> model().field(i).create());
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

    private class ModelBuilder {

        private final Model<T> model;

        public ModelBuilder() {
            model = TypeDefinition.getInstance(modelClass()).newInstance();
        }

        public ModelBuilder field(int index) {
            T value = value(index);
            model.field = value;
            model.set.add(value);
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
