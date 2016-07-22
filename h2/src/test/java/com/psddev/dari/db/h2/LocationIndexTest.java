package com.psddev.dari.db.h2;

import com.psddev.dari.db.Location;
import org.junit.Test;

public class LocationIndexTest extends AbstractSpatialIndexTest<Location> {

    @Override
    protected Class<? extends Model<Location>> modelClass() {
        return Foo.class;
    }

    @Override
    protected Location value(int index) {
        return new Location(index, index);
    }

    @Override
    @Test
    public void closest() {
        createSortTestModels();
        assertOrder(false, query() .sortClosest("field", new Location(0, 0)));
    }

    @Override
    @Test
    public void farthest() {
        createSortTestModels();
        assertOrder(true, query().sortFarthest("field", new Location(0, 0)));
    }

    public static class Foo extends Model<Location> {
    }
}
