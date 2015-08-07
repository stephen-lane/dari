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
 * Notes:
 * - Many of the tests combine both primitive and boxed tests, with two assertions.
 */
public class SqlDatabase_FieldTypes_Integer_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_FieldTypes_Integer_Test.class);

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();

    static IntegerBoxedExample boxed_1, boxed_2, boxed_n1, boxed_0, boxed_null;
    static IntegerPrimitiveExample primitive_1, primitive_2, primitive_n1, primitive_unset;

    @BeforeClass
    public static void beforeClass() {
        boxed_1 = new IntegerBoxedExample().setValue(1);
        boxed_1.save();
        boxed_2 = new IntegerBoxedExample().setValue(2);
        boxed_2.save();
        boxed_n1 = new IntegerBoxedExample().setValue(-1);
        boxed_n1.save();
        boxed_0 = new IntegerBoxedExample().setValue(0);
        boxed_0.save();
        boxed_null = new IntegerBoxedExample().setValue(null);
        boxed_null.save();

        primitive_1 = new IntegerPrimitiveExample().setValue(1);
        primitive_1.save();
        primitive_2 = new IntegerPrimitiveExample().setValue(2);
        primitive_2.save();
        primitive_n1 = new IntegerPrimitiveExample().setValue(-1);
        primitive_n1.save();
        primitive_unset = new IntegerPrimitiveExample();
        primitive_unset.save();
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
        assertEquals(boxed_1, Query.from(IntegerBoxedExample.class).where("value = ?", 1).first());

        assertEquals(primitive_2, Query.from(IntegerPrimitiveExample.class).where("value = ?", 2).first());
    }

    @Test
    public void test_equals_null() {
        assertEquals(boxed_null, Query.from(IntegerBoxedExample.class).where("value = missing").first());
    }

    @Test
    public void test_equals_unset() {
        assertEquals(null, Query.from(IntegerPrimitiveExample.class).where("value = missing").first());
        assertEquals(primitive_unset, Query.from(IntegerPrimitiveExample.class).where("value = 0").first());
    }

    @Test
    public void test_equals_none() {
        assertEquals(null, Query.from(IntegerBoxedExample.class).where("value = ?", -10000).first());
        assertEquals(null, Query.from(IntegerPrimitiveExample.class).where("value = ?", -10000).first());
    }

    /** OTHER COMPARISONS **/
    @Test
    public void test_not_equals() {
        assertEqualsUnordered(
                Arrays.asList(boxed_0, boxed_2, boxed_n1, boxed_null),
                Query.from(IntegerBoxedExample.class).where("value != ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(primitive_2, primitive_n1, primitive_unset),
                Query.from(IntegerPrimitiveExample.class).where("value != ?", 1).selectAll());
    }

    @Test
    public void test_less_than() {
        assertEqualsUnordered(
                Arrays.asList(boxed_n1, boxed_0),
                Query.from(IntegerBoxedExample.class).where("value < ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(primitive_n1, primitive_unset),
                Query.from(IntegerPrimitiveExample.class).where("value < ?", 1).selectAll());
    }

    @Test
    public void test_less_than_or_equals() {
        assertEqualsUnordered(
                Arrays.asList(boxed_n1, boxed_0, boxed_1),
                Query.from(IntegerBoxedExample.class).where("value <= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(primitive_n1, primitive_1, primitive_unset),
                Query.from(IntegerPrimitiveExample.class).where("value <= ?", 1).selectAll());
    }

    @Test
    public void test_greater_than() {
        assertEqualsUnordered(
                Arrays.asList(boxed_1, boxed_2),
                Query.from(IntegerBoxedExample.class).where("value > ?", 0).selectAll());

        assertEqualsUnordered(
                Arrays.asList(primitive_1, primitive_2),
                Query.from(IntegerPrimitiveExample.class).where("value > ?", 0).selectAll());
    }

    @Test
    public void test_greater_than_or_equals() {
        assertEqualsUnordered(
                Arrays.asList(boxed_0, boxed_1, boxed_2),
                Query.from(IntegerBoxedExample.class).where("value >= ?", 0).selectAll());

        assertEqualsUnordered(
                Arrays.asList(primitive_1, primitive_2, primitive_unset),
                Query.from(IntegerPrimitiveExample.class).where("value >= ?", 0).selectAll());
    }

    /** SORTING **/
    /* null sorts as lowest value */
    @Test
    public void test_sort_ascending() {
        assertEquals(
                Arrays.asList(boxed_null, boxed_n1, boxed_0, boxed_1, boxed_2),
                Query.from(IntegerBoxedExample.class).sortAscending("value").selectAll());

        assertEquals(
                Arrays.asList(primitive_n1, primitive_unset, primitive_1, primitive_2),
                Query.from(IntegerPrimitiveExample.class).sortAscending("value").selectAll());

    }

    @Test
    public void test_sort_descending() {
        assertEquals(
                Arrays.asList(boxed_2, boxed_1, boxed_0, boxed_n1, boxed_null),
                Query.from(IntegerBoxedExample.class).sortDescending("value").selectAll());

        assertEquals(
                Arrays.asList(primitive_2, primitive_1, primitive_unset, primitive_n1),
                Query.from(IntegerPrimitiveExample.class).sortDescending("value").selectAll());
    }

    /** VALUE **/
    @Test
    public void test_value() {
        assertEquals((Integer)1, Query.from(IntegerBoxedExample.class).where("id = ?", boxed_1).first().getValue());
        assertEquals(1, Query.from(IntegerPrimitiveExample.class).where("id = ?", primitive_1).first().getValue());
    }

    @Test
    public void test_value_null_correct() {
        assertEquals(null, Query.from(IntegerBoxedExample.class).where("id = ?", boxed_null).first().getValue());
    }

    @Test
    public void test_value_unset_correct() {
        assertEquals(0, Query.from(IntegerPrimitiveExample.class).where("id = ?", primitive_unset).first().getValue());
    }

    /** TEST CLASSES **/

    public static class IntegerBoxedExample extends Record {
        @Indexed
        private Integer value;

        public Integer getValue() {
            return value;
        }

        public IntegerBoxedExample setValue(Integer value) {
            this.value = value; return this;
        }
    }

    public static class IntegerPrimitiveExample extends Record {
        @Indexed
        private int value;

        public int getValue() {
            return value;
        }

        public IntegerPrimitiveExample setValue(int value) {
            this.value = value;
            return this;
        }
    }

}
