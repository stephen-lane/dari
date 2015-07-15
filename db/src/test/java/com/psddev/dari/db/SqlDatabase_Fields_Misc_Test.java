package com.psddev.dari.db;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/8/15.
 * General tests for field related functionality that doesn't fit elsewhere
 */
public class SqlDatabase_Fields_Misc_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_Fields_Misc_Test.class);

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();


    @BeforeClass
    public static void beforeClass() {}

    @AfterClass
    public static void afterClass() {}

    @Before
    public void before() {
        LOGGER.info("Running test [{}]", name.getMethodName());
    }

    @After
    public void after() {}


    /** Indexed **/
    public static class IndexedExample extends Record {
        private int unindexed = 1;
        @Recordable.Indexed
        private int indexed = 2;
        transient int unsaved = 3;
    }

    static IndexedExample indexedExample_1;

    @BeforeClass
    public static void beforeClass_indexed() {
        indexedExample_1 = new IndexedExample();
        indexedExample_1.save();
    }


    @Test
    public void test_indexed() {
        IndexedExample result = Query.from(IndexedExample.class).where("indexed = 2").first();

        assertEquals(indexedExample_1, result);
    }

    @Test (expected = com.psddev.dari.db.Query.NoIndexException.class)
    public void test_unindexed() {
        Query.from(IndexedExample.class).where("unindexed = 2").first();
    }

    @Test (expected = com.psddev.dari.db.Query.NoFieldException.class)
    public void test_unsaved() {
        Query.from(IndexedExample.class).where("unsaved = 3").first();
    }

    @Test (expected = com.psddev.dari.db.Query.NoFieldException.class)
    public void test_undefined() {
        Query.from(IndexedExample.class).where("undefined = 4").first();
    }

    // TODO: unknown
    
}
