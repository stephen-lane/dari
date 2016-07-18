package com.psddev.dari.db.h2;

import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class GroupByTest extends AbstractTest {

    @BeforeClass
    public static void createModels() {
        for (int i = 0; i < 26; ++ i) {
            for (int j = 0; j < i; ++ j) {
                Foo foo = new Foo();
                StringBuilder text = new StringBuilder();

                for (int k = i; k > j; -- k) {
                    text.append((char) ('a' + k));
                }

                if (text.length() > 0) {
                    foo.text = text.toString();
                    foo.save();
                }
            }
        }
    }

    @Test
    public void firstLetter() {
        List<Grouping<Foo>> groupings = Query.from(Foo.class).groupBy("getFirstLetter");

        assertThat(
                groupings,
                hasSize(25));

        groupings.forEach(g -> {
            String firstLetter = (String) g.getKeys().get(0);

            assertThat(
                    firstLetter,
                    g.getCount(),
                    is((long) (firstLetter.charAt(0) - 'a')));
        });
    }

    public static class Foo extends Record {

        @Indexed(unique = true)
        @Required
        public String text;

        @Indexed
        public String getFirstLetter() {
            return text.substring(0, 1);
        }
    }
}
