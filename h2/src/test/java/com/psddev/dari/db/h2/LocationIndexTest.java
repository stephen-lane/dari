package com.psddev.dari.db.h2;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import org.junit.Test;

public class LocationIndexTest extends AbstractIndexTest<LocationModel, Location> {

    @Override
    protected Class<LocationModel> modelClass() {
        return LocationModel.class;
    }

    @Override
    protected Location value(int index) {
        return new Location(index, index);
    }

    @Test
    public void eqRegion() {
        createCompareTestModels();
        assertCount(5, "one = ?", Region.sphericalCircle(0.0d, 0.0d, 5.5d));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void containsNull() {
        createCompareTestModels();
        query().and("one contains ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("one startsWith ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void gt() {
        createCompareTestModels();
        query().where("one > ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void gtNull() {
        createCompareTestModels();
        query().and("one > ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ge() {
        createCompareTestModels();
        query().where("one >= ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void geNull() {
        createCompareTestModels();
        query().and("one >= ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void lt() {
        createCompareTestModels();
        query().where("one < ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ltNull() {
        createCompareTestModels();
        query().and("one < ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void le() {
        createCompareTestModels();
        query().where("one <= ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void leNull() {
        createCompareTestModels();
        query().and("one <= ?", (Object) null).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ascending() {
        createSortTestModels();
        query().sortAscending("one").count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void descending() {
        createSortTestModels();
        query().sortDescending("one").count();
    }

    @Override
    @Test
    public void closest() {
        createSortTestModels();
        assertOrder(false, query() .sortClosest("one", new Location(0, 0)));
    }

    @Override
    @Test
    public void farthest() {
        createSortTestModels();
        assertOrder(true, query().sortFarthest("one", new Location(0, 0)));
    }
}
