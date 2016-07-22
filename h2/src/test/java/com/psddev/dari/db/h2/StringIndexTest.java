package com.psddev.dari.db.h2;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

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
    public void contains() {
        Foo foo = new Foo();

        foo.field = "abcde";
        foo.save();

        assertThat(
                query().where("field contains ?", "bcd").first(),
                is(foo));
    }

    @Override
    @Test
    public void startsWith() {
        Foo foo = new Foo();

        foo.field = "abcde";
        foo.save();

        assertThat(
                query().where("field startsWith ?", "abc").first(),
                is(foo));
    }

    @Override
    @Test
    public void invalidValue() {
    }

    public static class Foo extends Model<String> {
    }
}
