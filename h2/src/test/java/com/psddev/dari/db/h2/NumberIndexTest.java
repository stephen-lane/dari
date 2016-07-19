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
    @Test
    public void invalidValue() {
    }

    public static class Foo extends Model<Double> {
    }
}
