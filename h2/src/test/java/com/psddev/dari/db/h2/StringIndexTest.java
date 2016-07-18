package com.psddev.dari.db.h2;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class StringIndexTest extends AbstractTest {

    @BeforeClass
    public static void createModels() {
        new Foo().save();

        for (int i = 0; i < 26; ++ i) {
            Foo foo = new Foo();

            foo.text = String.valueOf((char) ('a' + i));
            foo.save();
        }
    }

    @Test
    public void missing() {
        assertThat(
                Query.from(Foo.class).where("text = missing").count(),
                is(1L));
    }

    @Test
    public void notMissing() {
        assertThat(
                Query.from(Foo.class).where("text != missing").count(),
                is(26L));
    }

    public static class Foo extends Record {

        @Indexed
        public String text;
    }
}
