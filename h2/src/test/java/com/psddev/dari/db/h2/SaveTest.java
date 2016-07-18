package com.psddev.dari.db.h2;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import com.psddev.dari.db.State;
import com.psddev.dari.db.StateStatus;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SaveTest extends AbstractTest {

    @Test
    public void retrySaved() {
        Foo foo = new Foo();
        State fooState = foo.getState();
        UUID id = UUID.randomUUID();

        fooState.setId(id);
        fooState.setStatus(StateStatus.SAVED);
        foo.save();

        assertThat(
                Query.from(Foo.class).where("_id = ?", id).first(),
                notNullValue());
    }

    @Test
    public void retryNew() {
        Foo foo1 = new Foo();
        State foo1State = foo1.getState();
        UUID id = UUID.randomUUID();

        foo1State.setId(id);
        foo1.save();

        Foo foo2 = new Foo();

        foo2.getState().setId(id);
        foo2.text = "retry";
        foo2.save();

        assertThat(
                Query.from(Foo.class).where("_id = ?", id).first().text,
                is(foo2.text));
    }

    public static class Foo extends Record {

        public String text;
    }
}
