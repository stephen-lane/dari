package com.psddev.dari.db.h2;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.Grouping;
import com.psddev.dari.db.Query;
import com.psddev.dari.util.PaginatedResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ReadTest extends AbstractTest {

    private static Set<ReadModel> MODELS;

    @BeforeClass
    public static void createModels() {
        MODELS = new HashSet<>();

        for (int i = 0; i < 26; ++ i) {
            for (int j = 0; j < i; ++ j) {
                ReadModel model = new ReadModel();
                StringBuilder text = new StringBuilder();

                for (int k = i; k > j; -- k) {
                    text.append((char) ('a' + k));
                }

                if (text.length() > 0) {
                    model.text = text.toString();
                    model.save();
                    MODELS.add(model);
                }
            }
        }
    }

    @Test
    public void all() {
        assertThat(
                new HashSet<>(Query.from(ReadModel.class).selectAll()),
                is(MODELS));
    }

    @Test
    public void allGrouped() {
        List<Grouping<ReadModel>> groupings = Query.from(ReadModel.class).groupBy("getFirstLetter");

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

    @Test
    public void count() {
        assertThat(
                Query.from(ReadModel.class).count(),
                is((long) MODELS.size()));
    }

    @Test
    public void first() {
        assertThat(
                Query.from(ReadModel.class).first(),
                isIn(MODELS));
    }

    @Test
    public void iterable() {
    }

    @Test
    public void lastUpdate() {
        assertThat(
                Query.from(ReadModel.class).lastUpdate().getTime(),
                lessThan(Database.Static.getDefault().now()));
    }

    @Test
    public void partial() {
        PaginatedResult<ReadModel> result = Query.from(ReadModel.class).select(0, 1);

        assertThat(result.getCount(), is((long) MODELS.size()));
        assertThat(result.getItems(), hasSize(1));
    }

    @Test
    public void partialGrouped() {
    }
}
