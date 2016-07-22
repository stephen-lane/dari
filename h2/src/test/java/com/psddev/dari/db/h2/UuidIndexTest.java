package com.psddev.dari.db.h2;

import org.junit.Test;

import java.util.UUID;

public class UuidIndexTest extends AbstractIndexTest<UUID> {

    @Override
    protected Class<? extends Model<UUID>> modelClass() {
        return Foo.class;
    }

    @Override
    protected UUID value(int index) {
        return new UUID(0L, 41L * index);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void containsNull() {
        createCompareTestModels();
        query().and("field contains ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("field startsWith ?", (Object) null).count();
    }

    public static class Foo extends Model<UUID> {
    }
}
