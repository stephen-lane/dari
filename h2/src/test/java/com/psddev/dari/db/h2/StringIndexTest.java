package com.psddev.dari.db.h2;

import org.junit.Test;

public class StringIndexTest extends AbstractIndexTest<String> {

    @Override
    protected Class<? extends Model<String>> modelClass() {
        return Foo.class;
    }

    @Override
    protected String value(int index) {
        return String.valueOf((char) ('a' + index));
    }

    @Override
    @Test
    public void invalidValue() {
    }

    public static class Foo extends Model<String> {
    }
}
