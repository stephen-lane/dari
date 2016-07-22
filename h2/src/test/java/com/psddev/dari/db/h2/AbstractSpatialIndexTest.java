package com.psddev.dari.db.h2;

import org.junit.Test;

public abstract class AbstractSpatialIndexTest<T> extends AbstractIndexTest<T> {

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void gt() {
        createCompareTestModels();
        query().where("field > ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ge() {
        createCompareTestModels();
        query().where("field >= ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void lt() {
        createCompareTestModels();
        query().where("field < ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void le() {
        createCompareTestModels();
        query().where("field <= ?", value(0)).count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void ascending() {
        createSortTestModels();
        query().sortAscending("field").count();
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void descending() {
        createSortTestModels();
        query().sortDescending("field").count();
    }
}
