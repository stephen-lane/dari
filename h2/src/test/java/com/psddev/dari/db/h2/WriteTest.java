package com.psddev.dari.db.h2;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.Query;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class WriteTest extends AbstractTest {

    @After
    public void deleteModels() {
        createDeleteTestModels();
        Query.from(WriteModel.class).deleteAll();
    }

    @Test
    public void save() {
        WriteModel model = new WriteModel();
        model.save();
        assertThat(Query.from(WriteModel.class).first(), is(model));
    }

    @Test
    public void saveRetrySaved() {
        WriteModel model = new WriteModel();
        model.save();
        Query.from(WriteModel.class).deleteAll();
        model.save();
        assertThat(Query.from(WriteModel.class).first(), is(model));
    }

    @Test
    public void saveRetryNew() {
        WriteModel model1 = new WriteModel();
        model1.save();
        WriteModel model2 = new WriteModel();
        model2.getState().setId(model1.getId());
        model2.save();
        assertThat(Query.from(WriteModel.class).first(), is(model1));
    }

    private List<WriteModel> createDeleteTestModels() {
        List<WriteModel> models = new ArrayList<>();

        for (int i = 0; i < 5; ++ i) {
            WriteModel model = new WriteModel();
            model.save();
            models.add(model);
        }

        return models;
    }

    @Test
    public void deleteFirst() {
        List<WriteModel> models = createDeleteTestModels();
        Query.from(WriteModel.class).first().delete();
        assertThat(Query.from(WriteModel.class).count(), is((long) models.size() - 1));
    }

    @Test
    public void deleteAll() {
        createDeleteTestModels();
        Query.from(WriteModel.class).deleteAll();
        assertThat(Query.from(WriteModel.class).count(), is(0L));
    }

    @Test
    public void rollback() {
        Database database = Database.Static.getDefault();

        database.beginWrites();

        try {
            new WriteModel().save();

        } finally {
            database.endWrites();
        }

        assertThat(Query.from(WriteModel.class).count(), is(0L));
    }
}
