package com.psddev.dari.db.h2;

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

    public static class Foo extends Model<UUID> {
    }
}
