package com.psddev.dari.db.h2;

import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class StringIndexTest extends AbstractIndexTest<String> {

    @Override
    protected Class<? extends Model<String>> modelClass() {
        return StringModel.class;
    }

    @Override
    protected String value(int index) {
        return String.valueOf((char) ('a' + index));
    }

    @Override
    @Test
    public void contains() {
        StringModel model = new StringModel();

        model.field = "abcde";
        model.save();

        assertThat(
                query().where("field contains ?", "bcd").first(),
                is(model));
    }

    @Override
    @Test
    public void startsWith() {
        StringModel model = new StringModel();

        model.field = "abcde";
        model.save();

        assertThat(
                query().where("field startsWith ?", "abc").first(),
                is(model));
    }

    @Override
    @Test
    public void invalidValue() {
    }

    public static class StringModel extends Model<String> {
    }
}
