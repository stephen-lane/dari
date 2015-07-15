package com.psddev.dari.db;

import org.joda.time.DateTime;
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
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static com.psddev.dari.db.AssertUtils.assertEqualsUnordered;

/**
 * Created by rhseeger on 7/8/15.
 */
public class SqlDatabase_FieldTypes_Date_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_FieldTypes_Date_Test.class);

    public static DateTime NOW  = DateTime.now();
    public static Date DATE_YESTERDAY = NOW.minusDays(1).toDate();
    public static Date DATE_NOW = NOW.toDate();
    public static Date DATE_TOMORROW = NOW.plusDays(1).toDate();
    public static Date DATE_OTHER = NOW.plusDays(2).toDate();

    static DateExample yesterday, today, tomorrow;
    static DateExample noDate;

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();


    @BeforeClass
    public static void beforeClass() {
        yesterday = new DateExample().setWhen(DATE_YESTERDAY);
        yesterday.save();
        today = new DateExample().setWhen(DATE_NOW);
        today.save();
        tomorrow = new DateExample().setWhen(DATE_TOMORROW);
        tomorrow.save();
        noDate = new DateExample().setWhen(null);
        noDate.save();
    }

    @AfterClass
    public static void afterClass() {}

    @Before
    public void before() {
        LOGGER.info("Running test [{}]", name.getMethodName());
    }

    @After
    public void after() {}

    
    /** EQUALS **/
    @Test
    public void test_equals() {
        DateExample result = Query.from(DateExample.class).where("when = ?", DATE_NOW).first();

        assertEquals(today, result);
    }

    @Test
    public void test_equals_null() {
        DateExample result = Query.from(DateExample.class).where("when = missing").first();

        assertEquals(noDate, result);
    }

    @Test
    public void test_equals_none() {
        DateExample result = Query.from(DateExample.class).where("when = ?", DATE_OTHER).first();

        assertEquals(null, result);
    }

    /** OTHER COMPARISONS **/
    @Test
    public void test_not_equals() {
        List<DateExample> result = Query.from(DateExample.class).where("when != ?", DATE_NOW).selectAll();

        assertEqualsUnordered(Arrays.asList(yesterday, tomorrow, noDate), result);
    }

    @Test
    public void test_less_than() {
        List<DateExample> result = Query.from(DateExample.class).where("when < ?", DATE_NOW).selectAll();

        assertEqualsUnordered(Arrays.asList(yesterday), result);
    }

    @Test
    public void test_less_than_or_equals() {
        List<DateExample> result = Query.from(DateExample.class).where("when <= ?", DATE_NOW).selectAll();

        assertEqualsUnordered(Arrays.asList(yesterday, today), result);
    }

    @Test
    public void test_greater_than() {
        List<DateExample> result = Query.from(DateExample.class).where("when > ?", DATE_NOW).selectAll();

        assertEqualsUnordered(Arrays.asList(tomorrow), result);
    }

    @Test
    public void test_greater_than_or_equals() {
        List<DateExample> result = Query.from(DateExample.class).where("when >= ?", DATE_NOW).selectAll();

        assertEqualsUnordered(Arrays.asList(today, tomorrow), result);
    }

    /** SORTING **/
    /* null sorts as lowest value */
    @Test
    public void test_sort_ascending() {
        List<DateExample> result = Query.from(DateExample.class).sortAscending("when").selectAll();

        assertEquals(Arrays.asList(noDate, yesterday, today, tomorrow), result);
    }

    @Test
    public void test_sort_descending() {
        List<DateExample> result = Query.from(DateExample.class).sortDescending("when").selectAll();

        assertEquals(Arrays.asList(tomorrow, today, yesterday, noDate), result);
    }

    /** VALUE **/
    @Test
    public void test_value() {
        DateExample result = Query.from(DateExample.class).where("id = ?", today).first();

        assertEquals(DATE_NOW, result.getWhen());
    }
    @Test
    public void test_value_null_correct() {
        DateExample result = Query.from(DateExample.class).where("id = ?", noDate).first();

        assertEquals(null, result.getWhen());
    }


    /** TEST CLASSES **/
    public static class DateExample extends Record {
        @Indexed
        private Date when;

        public Date getWhen() {
            return when;
        }

        public DateExample setWhen(Date when) {
            this.when = when; return this;
        }
    }

}
