package com.psddev.dari.db.h2;

import com.psddev.dari.db.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class DeleteTest extends AbstractTest {

    private List<DeleteModel> models;

    @Before
    public void createModels() {
        models = new ArrayList<>();

        for (int i = 0; i < 5; ++ i) {
            DeleteModel model = new DeleteModel();
            model.save();
            models.add(model);
        }
    }

    @After
    public void deleteModels() {
        Query.from(DeleteModel.class).deleteAll();
    }

    @Test
    public void deleteFirst() {
        Query.from(DeleteModel.class).first().delete();
        assertThat(Query.from(DeleteModel.class).count(), is((long) models.size() - 1));
    }

    @Test
    public void deleteAll() {
        Query.from(DeleteModel.class).deleteAll();
        assertThat(Query.from(DeleteModel.class).count(), is(0L));
    }
}
