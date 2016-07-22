package com.psddev.dari.db.h2;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.Region;
import org.junit.Test;

public class RegionIndexTest extends AbstractIndexTest<RegionModel, Region> {

    @Override
    protected Class<RegionModel> modelClass() {
        return RegionModel.class;
    }

    @Override
    protected Region value(int index) {
        return Region.sphericalCircle(0.0d, 0.0d, index + 1);
    }

    @Override
    @Test
    public void contains() {
        createCompareTestModels();
        assertCount(total, "field contains ?", new Location(0.0d, 0.0d));
        assertCount(total - 1, "field contains ?", new Location(1.5d, 0.0d));
        assertCount(total, "field contains ?", Region.sphericalCircle(0.0d, 0.0d, 0.5d));
        assertCount(total - 1, "field contains ?", Region.sphericalCircle(0.0d, 0.0d, 1.5d));
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void startsWithNull() {
        createCompareTestModels();
        query().and("field startsWith ?", (Object) null).count();
    }

    @Test(expected = IllegalArgumentException.class)
    public void containsIllegal() {
        createCompareTestModels();
        query().where("field contains true").count();
    }

    @Test
    public void gtNumber() {
        createCompareTestModels();
        assertCount(total, "field > 0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void gtIllegal() {
        createCompareTestModels();
        query().where("field > true").count();
    }

    @Test
    public void geNumber() {
        createCompareTestModels();
        assertCount(total, "field >= 0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void geIllegal() {
        createCompareTestModels();
        query().where("field > true").count();
    }

    @Test
    public void ltNumber() {
        createCompareTestModels();
        assertCount(1, "field < 10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void ltIllegal() {
        createCompareTestModels();
        query().where("field < true").count();
    }

    @Test
    public void leNumber() {
        createCompareTestModels();
        assertCount(1, "field <= 10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void leIllegal() {
        createCompareTestModels();
        query().where("field <= true").count();
    }
}
