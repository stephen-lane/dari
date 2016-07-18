package com.psddev.dari.db.h2;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class CountTest extends AbstractTest {

    @BeforeClass
    public static void createModels() {
        for (int i = 0; i < 26; ++ i) {
            Foo foo = new Foo();
            StringBuilder text = new StringBuilder();

            for (int j = 0; j <= i; ++ j) {
                text.append((char) ('a' + j));
            }

            foo.text = text.toString();
            foo.save();
        }
    }

    @Test
    public void all() {
        assertThat(
                Query.from(Foo.class).count(),
                is(26L));
    }

    @Test
    public void contains() {
        assertThat(
                Query.from(Foo.class).where("text contains 'efg'").count(),
                is(20L));
    }

    @Test
    public void startsWith() {
        assertThat(
                Query.from(Foo.class).where("text startsWith 'abcdefg'").count(),
                is(20L));
    }

    @Test
    public void lt() {
        assertThat(
                Query.from(Foo.class).where("getLength < 6").count(),
                is(5L));
    }

    @Test
    public void le() {
        assertThat(
                Query.from(Foo.class).where("getLength <= 6").count(),
                is(6L));
    }

    @Test
    public void gt() {
        assertThat(
                Query.from(Foo.class).where("getLength > 6").count(),
                is(20L));
    }

    @Test
    public void ge() {
        assertThat(
                Query.from(Foo.class).where("getLength >= 6").count(),
                is(21L));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupported() {
        Query.from(Foo.class).where("text matches 'abc'").count();
    }

    public static class Foo extends Record {

        @Indexed(unique = true)
        @Required
        public String text;

        @Indexed
        public int getLength() {
            return text.length();
        }
    }
}
