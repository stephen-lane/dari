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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.psddev.dari.db.AssertUtils.assertEqualsUnordered;
import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/8/15.
 * Tests that deal with retrieving only a specific window of the results
 * .first
 * PaginatedResult
 *     .select(x,y)
 *     .hasNext()
 *     .getNextOffset()
 *
 */
public class SqlDatabase_Window_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_Window_Test.class);

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


    /** .first **/
    public static class FirstTestNone extends Record {}
    public static class FirstTestOne extends Record {}
    public static class FirstTestMultiple extends Record {
        @Indexed public int order;
        public FirstTestMultiple setOrder(int order) { this.order = order; return this; }
    }

    static private FirstTestOne firstTestOne;
    static private FirstTestMultiple firstTestMultiple_1, firstTestMultiple_2;

    @BeforeClass
    public static void beforeClass_abstract_instance() {
        firstTestOne = new FirstTestOne();
        firstTestOne.save();

        firstTestMultiple_1 = new FirstTestMultiple().setOrder(1);
        firstTestMultiple_1.save();
        firstTestMultiple_2 = new FirstTestMultiple().setOrder(2);
        firstTestMultiple_2.save();
    }


    @Test
    public void first_none() {
        FirstTestNone result = Query.from(FirstTestNone.class).first();

        assertEquals(null, result);
    }

    @Test
    public void first_one() {
        FirstTestOne result = Query.from(FirstTestOne.class).first();

        assertEquals(firstTestOne, result);
    }

    @Test
    public void first_multiple() {
        // We need to sort here to make sure we get back the one we want
        FirstTestMultiple result = Query.from(FirstTestMultiple.class).sortAscending("order").first();

        assertEquals(firstTestMultiple_1, result);
    }


}
