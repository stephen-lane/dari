package com.psddev.dari.db.h2;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Query;
import com.psddev.dari.util.TypeDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public abstract class AbstractIndexTest<M extends Model<M, T>, T> extends AbstractTest {

    protected int total;

    protected abstract Class<M> modelClass();

    protected abstract T value(int index);

    protected ModelBuilder model() {
        return new ModelBuilder();
    }

    @Before
    public void resetTotal() {
        total = 0;
    }

    protected Query<M> query() {
        return Query.from(modelClass());
    }

    @After
    public void deleteModels() {
        query().deleteAll();
    }

    @Test
    public void invalidValue() {
        M model = TypeDefinition.getInstance(modelClass()).newInstance();
        model.getState().put("field", new Object());
        model.save();

        assertThat(
                Query.from(modelClass()).first().field,
                nullValue());
    }

    protected void assertCount(long count, String predicate, Object... parameters) {
        assertThat(predicate, query().where(predicate, parameters).count(), is(count));
    }

    protected void createMissingTestModels() {
        T value0 = value(0);

        model().create();
        model().field(value0).create();
        model().set(value0).create();
        model().list(value0).create();
        model().field(value0).set(value0).create();
        model().field(value0).list(value0).create();
        model().set(value0).list(value0).create();
        model().all(value0).create();
    }

    protected void missing(String field, long count) {
        assertCount(count, field + " = missing");
        assertCount(total - count, field + " != missing");
    }

    @Test
    public void missing() {
        createMissingTestModels();
        missing("field", 4L);
        missing("set", 4L);
        missing("list", 4L);
    }

    protected void missingBoth(String field1, String field2, long count) {
        assertCount(count, field1 + " = missing and " + field2 + " = missing");
        assertCount(total - count, field1 + " != missing or " + field2 + " != missing");
    }

    @Test
    public void missingBoth() {
        createMissingTestModels();
        missingBoth("field", "set", 2L);
        missingBoth("field", "list", 2L);
        missingBoth("set", "list", 2L);
    }

    protected void missingEither(String field1, String field2, long count) {
        assertCount(count, field1 + " = missing or " + field2 + " = missing");
        assertCount(total - count, field1 + " != missing and " + field2 + " != missing");
    }

    @Test
    public void missingEither() {
        createMissingTestModels();
        missingEither("field", "set", 6L);
        missingEither("field", "list", 6L);
        missingEither("set", "list", 6L);
    }

    protected void createCompareTestModels() {
        IntStream.range(0, 5).forEach(i -> model().all(value(i)).create());
    }

    @Test
    public void eq() {
        createCompareTestModels();
        assertCount(1L, "field = ?", value(2));
        assertCount(1L, "set = ?", value(2));
        assertCount(1L, "list = ?", value(2));
    }

    @Test
    public void ne() {
        createCompareTestModels();
        assertCount(4L, "field != ?", value(2));
        assertCount(4L, "set != ?", value(2));
        assertCount(4L, "list != ?", value(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void contains() {
        createCompareTestModels();
        query().where("field contains ?", value(0)).count();
    }

    @Test
    public void containsNull() {
        createCompareTestModels();
        assertCount(0, "field contains ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void containsMissing() {
        createCompareTestModels();
        query().where("field contains missing").count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void startsWith() {
        createCompareTestModels();
        query().where("field startsWith ?", value(0)).count();
    }

    @Test
    public void startsWithNull() {
        createCompareTestModels();
        assertCount(0, "field startsWith ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void startsWithMissing() {
        createCompareTestModels();
        query().where("field startsWith missing").count();
    }

    @Test
    public void gt() {
        createCompareTestModels();
        assertCount(2L, "field > ?", value(2));
    }

    @Test
    public void gtNull() {
        createCompareTestModels();
        assertCount(0, "field > ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void gtMissing() {
        createCompareTestModels();
        query().where("field > missing").count();
    }

    @Test
    public void ge() {
        createCompareTestModels();
        assertCount(3L, "field >= ?", value(2));
    }

    @Test
    public void geNull() {
        createCompareTestModels();
        assertCount(0, "field >= ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void geMissing() {
        createCompareTestModels();
        query().where("field >= missing").count();
    }

    @Test
    public void lt() {
        createCompareTestModels();
        assertCount(2L, "field < ?", value(2));
    }

    @Test
    public void ltNull() {
        createCompareTestModels();
        assertCount(0, "field < ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ltMissing() {
        createCompareTestModels();
        query().where("field < missing").count();
    }

    @Test
    public void le() {
        createCompareTestModels();
        assertCount(3L, "field <= ?", value(2));
    }

    @Test
    public void leNull() {
        createCompareTestModels();
        assertCount(0, "field <= ?", (Object) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void leMissing() {
        createCompareTestModels();
        query().where("field <= missing").count();
    }

    protected void createSortTestModels() {
        for (int i = 0, size = 26; i < size; ++ i) {
            model().all(value(i % 2 == 0 ? i : size - i)).create();
        }
    }

    protected void assertOrder(boolean reverse, Query<M> query) {
        List<M> models = query.selectAll();

        assertThat(models, hasSize(total));

        for (int i = 0; i < total; ++ i) {
            assertThat(models.get(i).field, is(value(reverse ? total - 1 - i : i)));
        }
    }

    @Test
    public void ascending() {
        createSortTestModels();
        assertOrder(false, query().sortAscending("field"));
    }

    @Test
    public void descending() {
        createSortTestModels();
        assertOrder(true, query().sortDescending("field"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void closest() {
        createSortTestModels();
        query().sortClosest("field", new Location(0, 0)).first();
    }

    @Test(expected = IllegalArgumentException.class)
    public void farthest() {
        createSortTestModels();
        query().sortFarthest("field", new Location(0, 0)).first();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void sortUnknown() {
        createSortTestModels();
        query().sort("unknown", "field").first();
    }

    protected class ModelBuilder {

        private final M model;

        public ModelBuilder() {
            model = TypeDefinition.getInstance(modelClass()).newInstance();
        }

        public ModelBuilder all(T value) {
            model.field = value;
            model.set.add(value);
            model.list.add(value);
            model.list.add(value);
            return this;
        }

        public ModelBuilder field(T value) {
            model.field = value;
            return this;
        }

        public ModelBuilder set(T value) {
            model.set.add(value);
            return this;
        }

        public ModelBuilder list(T value) {
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
