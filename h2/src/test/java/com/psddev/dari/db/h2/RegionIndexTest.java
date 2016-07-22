package com.psddev.dari.db.h2;

import com.psddev.dari.db.Region;

public class RegionIndexTest extends AbstractIndexTest<Region> {

    @Override
    protected Class<? extends Model<Region>> modelClass() {
        return Foo.class;
    }

    @Override
    protected Region value(int index) {
        return Region.sphericalCircle(0.0d, 0.0d, index + 1);
    }

    public static class Foo extends Model<Region> {
    }
}
