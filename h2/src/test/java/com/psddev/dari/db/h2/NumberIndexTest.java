package com.psddev.dari.db.h2;

import org.junit.Test;

public class NumberIndexTest extends AbstractIndexTest<Double> {

    @Override
    protected Class<? extends Model<Double>> modelClass() {
        return Foo.class;
    }

    @Override
    protected Double value(int index) {
        return (double) index;
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void containsNull() {
        createCompareTestModels();
        query().and("field contains ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("field startsWith ?", (Object) null).count();
    }

    @Override
    @Test
    public void invalidValue() {
    }

    public static class Foo extends Model<Double> {
    }
}
