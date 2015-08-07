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
import java.util.List;

import static com.psddev.dari.db.AssertUtils.assertEqualsUnordered;
import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/8/15.
 */
public class SqlDatabase_FieldTypes_String_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_FieldTypes_String_Test.class);

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();


    static StringExample instanceAAA, instanceBBB, instanceCCC, instanceNull;

    @BeforeClass
    public static void beforeClass() {
        instanceAAA = new StringExample().setValue("AAA");
        instanceAAA.save();
        instanceBBB = new StringExample().setValue("BBB");
        instanceBBB.save();
        instanceCCC = new StringExample().setValue("CCC");
        instanceCCC.save();
        instanceNull = new StringExample().setValue(null);
        instanceNull.save();
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
        StringExample result = Query.from(StringExample.class).where("value = ?", "AAA").first();

        assertEquals(instanceAAA, result);
    }

    @Test
    public void test_equals_null() {
        StringExample result = Query.from(StringExample.class).where("value = missing").first();

        assertEquals(instanceNull, result);
    }

    @Test
    public void test_equals_none() {
        StringExample result = Query.from(StringExample.class).where("value = ?", "UNKNOWN").first();

        assertEquals(null, result);
    }

    /** OTHER COMPARISONS **/
    @Test
    public void test_not_equals() {
        List<StringExample> result = Query.from(StringExample.class).where("value != ?", "AAA").selectAll();

        assertEqualsUnordered(Arrays.asList(instanceBBB, instanceCCC, instanceNull), result);
    }

    @Test
    public void test_less_than() {
        List<StringExample> result = Query.from(StringExample.class).where("value < ?", "BBB").selectAll();

        assertEqualsUnordered(Arrays.asList(instanceAAA), result);
    }

    @Test
    public void test_less_than_or_equals() {
        List<StringExample> result = Query.from(StringExample.class).where("value <= ?", "BBB").selectAll();

        assertEqualsUnordered(Arrays.asList(instanceAAA, instanceBBB), result);
    }

    @Test
    public void test_greater_than() {
        List<StringExample> result = Query.from(StringExample.class).where("value > ?", "BBB").selectAll();

        assertEqualsUnordered(Arrays.asList(instanceCCC), result);
    }

    @Test
    public void test_greater_than_or_equals() {
        List<StringExample> result = Query.from(StringExample.class).where("value >= ?", "BBB").selectAll();

        assertEqualsUnordered(Arrays.asList(instanceBBB, instanceCCC), result);
    }

    /** SORTING **/
    /* null sorts as lowest value */
    @Test
    public void test_sort_ascending() {
        List<StringExample> result = Query.from(StringExample.class).sortAscending("value").selectAll();

        assertEquals(Arrays.asList(instanceNull, instanceAAA, instanceBBB, instanceCCC), result);
    }

    @Test
    public void test_sort_descending() {
        List<StringExample> result = Query.from(StringExample.class).sortDescending("value").selectAll();

        assertEquals(Arrays.asList(instanceCCC, instanceBBB, instanceAAA, instanceNull), result);
    }

    /** VALUE **/
    @Test
    public void test_value() {
        StringExample result = Query.from(StringExample.class).where("id = ?", instanceAAA).first();

        assertEquals("AAA", result.getValue());
    }
    @Test
    public void test_value_null_correct() {
        StringExample result = Query.from(StringExample.class).where("id = ?", instanceNull).first();

        assertEquals(null, result.getValue());
    }


    /** TEST CLASSES **/
    public static class StringExample extends Record {
        @Indexed
        private String value;

        public String getValue() {
            return value;
        }

        public StringExample setValue(String value) {
            this.value = value; return this;
        }
    }
    
}
