package com.psddev.dari.db.h2;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class StringIndexTest extends AbstractIndexTest<StringModel, String> {

    @Override
    protected Class<StringModel> modelClass() {
        return StringModel.class;
    }

    @Override
    protected String value(int index) {
        return String.valueOf((char) ('a' + index));
    }

    @Override
    @Test
    public void contains() {
        StringModel model = model().one("abcde").create();
        assertThat(
                query().where("one contains ?", "bcd").first(),
                is(model));
    }

    @Override
    @Test
    public void startsWith() {
        StringModel model = model().one("abcde").create();
        assertThat(
                query().where("one startsWith ?", "abc").first(),
                is(model));
    }

    @Override
    @Test
    public void invalidValue() {
    }
}
