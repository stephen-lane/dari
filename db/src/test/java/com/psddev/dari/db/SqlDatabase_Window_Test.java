package com.psddev.dari.db;

import com.psddev.dari.util.PaginatedResult;
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
    public static void beforeClass_first() {
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


    /*** PaginatedResult ***/
    /*** Note that we're not testing PaginatedResult here.
       * Rather, we're testing that it's setup correctly by the database query
       * And that the results are correct ***/

    public static class PaginatedNone extends Record {}
    public static class PaginatedItems extends Record {
        @Indexed public int order;
        public PaginatedItems setOrder(int order) { this.order = order; return this; }
    }

    static PaginatedItems paginatedItems_1, paginatedItems_2, paginatedItems_3, paginatedItems_4, paginatedItems_5;

    @BeforeClass
    public static void beforeClass_paginated() {
        paginatedItems_1 = new PaginatedItems().setOrder(1);
        paginatedItems_1.save();
        paginatedItems_2 = new PaginatedItems().setOrder(2);
        paginatedItems_2.save();
        paginatedItems_3 = new PaginatedItems().setOrder(2);
        paginatedItems_3.save();
        paginatedItems_4 = new PaginatedItems().setOrder(2);
        paginatedItems_4.save();
        paginatedItems_5 = new PaginatedItems().setOrder(2);
        paginatedItems_5.save();
    }


    /** .select(long offset, int limit) **/
    @Test
    public void select_no_results() {
        PaginatedResult<PaginatedNone> result = Query.from(PaginatedNone.class).select(0,10);

        assertEquals(Collections.<PaginatedNone>emptyList(), result.getItems());
    }

    @Test
    public void select_subset() {
        PaginatedResult<PaginatedItems> result = Query.from(PaginatedItems.class).sortAscending("order").select(0,2);

        assertEquals(Arrays.asList(paginatedItems_1, paginatedItems_2), result.getItems());
    }

    @Test
    public void select_none() {
        PaginatedResult<PaginatedItems> result = Query.from(PaginatedItems.class).sortAscending("order").select(0,0);

        assertEquals(Collections.<PaginatedNone>emptyList(), result.getItems());
    }

    @Test
    public void select_all() {
        PaginatedResult<PaginatedItems> result = Query.from(PaginatedItems.class).sortAscending("order").select(0,5);

        assertEquals(Arrays.asList(paginatedItems_1, paginatedItems_2, paginatedItems_3, paginatedItems_4, paginatedItems_5), result.getItems());
    }

    @Test
    public void select_limit_over_number() {
        PaginatedResult<PaginatedItems> result = Query.from(PaginatedItems.class).sortAscending("order").select(0,10);

        assertEquals(Arrays.asList(paginatedItems_1, paginatedItems_2, paginatedItems_3, paginatedItems_4, paginatedItems_5), result.getItems());
    }

    @Test
    public void select_subset_middle() {
        PaginatedResult<PaginatedItems> result = Query.from(PaginatedItems.class).sortAscending("order").select(1,2);

        assertEquals(Arrays.asList(paginatedItems_2, paginatedItems_3), result.getItems());
    }

    @Test
    public void select_offset_over_number() {
        PaginatedResult<PaginatedItems> result = Query.from(PaginatedItems.class).sortAscending("order").select(10,2);

        assertEquals(Collections.<PaginatedNone>emptyList(), result.getItems());
    }

    /* ERROR CASES */

    // TODO: This should really be caught as an error before hitting the database
    @Test (expected = SqlDatabaseException.class)
    public void select_offset_negative() {
        Query.from(PaginatedItems.class).select(-10,2);
    }

    // TODO: This should really be caught as an error before hitting the database
    @Test (expected = SqlDatabaseException.class)
    public void select_limit_negative() {
        Query.from(PaginatedItems.class).select(2,-10);
    }

    // TODO: these
    /** result.getCount() **/

    /** result.getFirstOffset() **/
    /** result.getFirstItemIndex() **/

    /** result.hasPrevious() **/
    /** result.getHasPrevious() **/
    /** result.getPreviousOffset() **/

    /** result.hasNext() **/
    /** result.getHasNext() **/
    /** result.getNextOffset() **/

    /** result.getLastItemIndex() **/
    /** result.getLastOffset() **/

    /** result.hasPages() **/
    /** result.getHasPages() **/
    /** result.getPageCount() **/
    /** result.getPageIndex() **/

    /** result.getLimit() **/
    /** result.getOffset() **/


}
