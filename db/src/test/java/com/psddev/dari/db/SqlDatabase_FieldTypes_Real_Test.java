package com.psddev.dari.db;

import com.psddev.dari.util.ObjectUtils;
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

import static com.psddev.dari.db.AssertUtils.assertEqualsUnordered;
import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/8/15.
 */
public class SqlDatabase_FieldTypes_Real_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_FieldTypes_Real_Test.class);

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();

    public static final Float DELTA_FLOAT = 0.0000001f;
    public static final Float DELTA_DOUBLE = 0.0000001f;

    static FloatBoxedExample float_boxed_1, float_boxed_1_2, float_boxed_neg1, float_boxed_0, float_boxed_null;
    static FloatPrimitiveExample float_primitive_1, float_primitive_1_2, float_primitive_neg1, float_primitive_unset;
    static DoubleBoxedExample double_boxed_1, double_boxed_1_2, double_boxed_neg1, double_boxed_0, double_boxed_null;
    static DoublePrimitiveExample double_primitive_1, double_primitive_1_2, double_primitive_neg1, double_primitive_unset;

    @BeforeClass
    public static void beforeClass() {
        float_boxed_1 = new FloatBoxedExample().setValue(1f);
        float_boxed_1.save();
        float_boxed_1_2 = new FloatBoxedExample().setValue(1.2f);
        float_boxed_1_2.save();
        float_boxed_neg1 = new FloatBoxedExample().setValue(-1f);
        float_boxed_neg1.save();
        float_boxed_0 = new FloatBoxedExample().setValue(0f);
        float_boxed_0.save();
        float_boxed_null = new FloatBoxedExample().setValue(null);
        float_boxed_null.save();

        float_primitive_1 = new FloatPrimitiveExample().setValue(1f);
        float_primitive_1.save();
        float_primitive_1_2 = new FloatPrimitiveExample().setValue(1.2f);
        float_primitive_1_2.save();
        float_primitive_neg1 = new FloatPrimitiveExample().setValue(-1f);
        float_primitive_neg1.save();
        float_primitive_unset = new FloatPrimitiveExample();
        float_primitive_unset.save();

        double_boxed_1 = new DoubleBoxedExample().setValue(1d);
        double_boxed_1.save();
        double_boxed_1_2 = new DoubleBoxedExample().setValue(1.2d);
        double_boxed_1_2.save();
        double_boxed_neg1 = new DoubleBoxedExample().setValue(-1d);
        double_boxed_neg1.save();
        double_boxed_0 = new DoubleBoxedExample().setValue(0d);
        double_boxed_0.save();
        double_boxed_null = new DoubleBoxedExample().setValue(null);
        double_boxed_null.save();

        double_primitive_1 = new DoublePrimitiveExample().setValue(1d);
        double_primitive_1.save();
        double_primitive_1_2 = new DoublePrimitiveExample().setValue(1.2d);
        double_primitive_1_2.save();
        double_primitive_neg1 = new DoublePrimitiveExample().setValue(-1d);
        double_primitive_neg1.save();
        double_primitive_unset = new DoublePrimitiveExample();
        double_primitive_unset.save();
    }

    @AfterClass
    public static void afterClass() {}

    @Before
    public void before() {
        LOGGER.info("Running test [{}]", name.getMethodName());
    }

    @After
    public void after() {}

    /* TODO: update this to show that ("value = ?", double) is busted... but ("value = double") works fine
     *       email Hyoo with progress
     */
    @Test
    public void test_test() {
        System.out.println("RHS: " + ObjectUtils.toJson(Query.from(FloatBoxedExample.class).selectAll()));
        // both of these return the 1.0 value object
        System.out.println("RHS: " + ObjectUtils.toJson(Query.from(FloatBoxedExample.class).where("value = ?", 1.2).selectAll()));
        System.out.println("RHS: " + ObjectUtils.toJson(Query.from(FloatBoxedExample.class).where("value = ?", (Float) 1.2f).selectAll()));
        System.out.println("RHS: " + ObjectUtils.toJson(Query.from(FloatBoxedExample.class).where("value = 1.2").selectAll()));
    }

    /** EQUALS **/
    // All fo these currently return the 1.0 value instance, not the 1.2... bug in dari
    @Test
    public void test_equals_float_boxed() {
        assertEquals(float_boxed_1_2, Query.from(FloatBoxedExample.class).where("value = ?", 1.2).first());
    }

    @Test
    public void test_equals_float_primitive() {
        assertEquals(float_primitive_1_2, Query.from(FloatPrimitiveExample.class).where("value = ?", 1.2).first());
    }

    @Test
    public void test_equals_double_boxed() {
        assertEquals(double_boxed_1_2, Query.from(DoubleBoxedExample.class).where("value = ?", 1.2).first());
    }

    @Test
    public void test_equals_double_primitive() {
        assertEquals(double_primitive_1_2, Query.from(DoublePrimitiveExample.class).where("value = ?", 1.2).first());
    }

    /** EQUALS with the value inside the string **/
    // Same as the previous four, but we can see it works if we put the 1.2 inside the string
    // Likely more of a parser issue
    @Test
    public void test_equals_instring_float_boxed() {
        assertEquals(float_boxed_1_2, Query.from(FloatBoxedExample.class).where("value = 1.2").first());
    }

    @Test
    public void test_equals_instring_float_primitive() {
        assertEquals(float_primitive_1_2, Query.from(FloatPrimitiveExample.class).where("value = 1.2").first());
    }

    @Test
    public void test_equals_instring_double_boxed() {
        assertEquals(double_boxed_1_2, Query.from(DoubleBoxedExample.class).where("value = 1.2").first());
    }

    @Test
    public void test_equals_instring_double_primitive() {
        assertEquals(double_primitive_1_2, Query.from(DoublePrimitiveExample.class).where("value = 1.2").first());
    }


    @Test
    public void test_equals_null() {
        assertEquals(float_boxed_null, Query.from(FloatBoxedExample.class).where("value = missing").first());
        assertEquals(double_boxed_null, Query.from(DoubleBoxedExample.class).where("value = missing").first());
    }

    @Test
    public void test_equals_unset() {
        assertEquals(float_primitive_unset, Query.from(FloatPrimitiveExample.class).where("value = 0").first());
        assertEquals(double_primitive_unset, Query.from(DoublePrimitiveExample.class).where("value = 0").first());
    }

    @Test
    public void test_equals_none() {
        assertEquals(null, Query.from(FloatBoxedExample.class).where("value = ?", -10000).first());
        assertEquals(null, Query.from(FloatPrimitiveExample.class).where("value = ?", -10000).first());
        assertEquals(null, Query.from(DoubleBoxedExample.class).where("value = ?", -10000).first());
        assertEquals(null, Query.from(DoublePrimitiveExample.class).where("value = ?", -10000).first());
    }

    /** OTHER COMPARISONS **/
    @Test
    public void test_not_equals() {
        assertEqualsUnordered(
                Arrays.asList(float_boxed_1, float_boxed_1_2, float_boxed_0, float_boxed_null),
                Query.from(FloatBoxedExample.class).where("value != ?", -1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(float_primitive_1, float_primitive_1_2, float_primitive_unset),
                Query.from(FloatPrimitiveExample.class).where("value != ?", -1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_boxed_1, double_boxed_1_2, double_boxed_0, double_boxed_null),
                Query.from(DoubleBoxedExample.class).where("value != ?", -1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_primitive_1, double_primitive_1_2, double_primitive_unset),
                Query.from(DoublePrimitiveExample.class).where("value != ?", -1).selectAll());
    }

    @Test
    public void test_less_than() {
        assertEqualsUnordered(
                Arrays.asList(float_boxed_neg1, float_boxed_0),
                Query.from(FloatBoxedExample.class).where("value < ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(float_primitive_neg1, float_primitive_unset),
                Query.from(FloatPrimitiveExample.class).where("value < ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_boxed_neg1, double_boxed_0),
                Query.from(DoubleBoxedExample.class).where("value < ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_primitive_neg1, double_primitive_unset),
                Query.from(DoublePrimitiveExample.class).where("value < ?", 1).selectAll());
    }

    @Test
    public void test_less_than_or_equals() {
        assertEqualsUnordered(
                Arrays.asList(float_boxed_1, float_boxed_neg1, float_boxed_0),
                Query.from(FloatBoxedExample.class).where("value <= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(float_primitive_1, float_primitive_neg1, float_primitive_unset),
                Query.from(FloatPrimitiveExample.class).where("value <= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_boxed_1, double_boxed_neg1, double_boxed_0),
                Query.from(DoubleBoxedExample.class).where("value <= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_primitive_1, double_primitive_neg1, double_primitive_unset),
                Query.from(DoublePrimitiveExample.class).where("value <= ?", 1).selectAll());
    }

    @Test
    public void test_greater_than() {
        assertEqualsUnordered(
                Arrays.asList(float_boxed_1_2),
                Query.from(FloatBoxedExample.class).where("value > ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(float_primitive_1_2),
                Query.from(FloatPrimitiveExample.class).where("value > ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_boxed_1_2),
                Query.from(DoubleBoxedExample.class).where("value > ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_primitive_1_2),
                Query.from(DoublePrimitiveExample.class).where("value > ?", 1).selectAll());
    }

    @Test
    public void test_greater_than_or_equals() {
        assertEqualsUnordered(
                Arrays.asList(float_boxed_1, float_boxed_1_2),
                Query.from(FloatBoxedExample.class).where("value >= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(float_primitive_1, float_primitive_1_2),
                Query.from(FloatPrimitiveExample.class).where("value >= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_boxed_1, double_boxed_1_2),
                Query.from(DoubleBoxedExample.class).where("value >= ?", 1).selectAll());

        assertEqualsUnordered(
                Arrays.asList(double_primitive_1, double_primitive_1_2),
                Query.from(DoublePrimitiveExample.class).where("value >= ?", 1).selectAll());
    }


    /** SORTING **/
    /* null sorts as lowest value */
    @Test
    public void test_sort_ascending() {
        assertEquals(
                Arrays.asList(float_boxed_null, float_boxed_neg1, float_boxed_0, float_boxed_1, float_boxed_1_2),
                Query.from(FloatBoxedExample.class).sortAscending("value").selectAll());

        assertEquals(
                Arrays.asList(float_primitive_neg1, float_primitive_unset, float_primitive_1, float_primitive_1_2),
                Query.from(FloatPrimitiveExample.class).sortAscending("value").selectAll());

        assertEquals(
                Arrays.asList(double_boxed_null, double_boxed_neg1, double_boxed_0, double_boxed_1, double_boxed_1_2),
                Query.from(DoubleBoxedExample.class).sortAscending("value").selectAll());

        assertEquals(
                Arrays.asList(double_primitive_neg1, double_primitive_unset, double_primitive_1, double_primitive_1_2),
                Query.from(DoublePrimitiveExample.class).sortAscending("value").selectAll());
    }

    @Test
    public void test_sort_descending() {
        assertEquals(
                Arrays.asList(float_boxed_1_2, float_boxed_1, float_boxed_0, float_boxed_neg1, float_boxed_null),
                Query.from(FloatBoxedExample.class).sortDescending("value").selectAll());

        assertEquals(
                Arrays.asList(float_primitive_1_2, float_primitive_1, float_primitive_unset, float_primitive_neg1),
                Query.from(FloatPrimitiveExample.class).sortDescending("value").selectAll());

        assertEquals(
                Arrays.asList(double_boxed_1_2, double_boxed_1, double_boxed_0, double_boxed_neg1, double_boxed_null),
                Query.from(DoubleBoxedExample.class).sortDescending("value").selectAll());

        assertEquals(
                Arrays.asList(double_primitive_1_2, double_primitive_1, double_primitive_unset, double_primitive_neg1),
                Query.from(DoublePrimitiveExample.class).sortDescending("value").selectAll());
    }

    /** VALUE **/
    @Test
    public void test_value() {
        assertEquals((Float)1.2f, Query.from(FloatBoxedExample.class).where("id = ?", float_boxed_1_2).first().getValue());
        assertEquals(1.2f, Query.from(FloatPrimitiveExample.class).where("id = ?", float_primitive_1_2).first().getValue(), DELTA_FLOAT);
        assertEquals((Double)1.2d, Query.from(DoubleBoxedExample.class).where("id = ?", double_boxed_1_2).first().getValue());
        assertEquals(1.2d, Query.from(DoublePrimitiveExample.class).where("id = ?", double_primitive_1_2).first().getValue(), DELTA_DOUBLE);
    }

    @Test
    public void test_value_null_correct() {
        assertEquals(null, Query.from(FloatBoxedExample.class).where("id = ?", float_boxed_null).first().getValue());
        assertEquals(null, Query.from(DoubleBoxedExample.class).where("id = ?", double_boxed_null).first().getValue());
    }

    @Test
    public void test_value_unset_correct() {
        assertEquals(0, Query.from(FloatPrimitiveExample.class).where("id = ?", float_primitive_unset).first().getValue(), DELTA_FLOAT);
        assertEquals(0, Query.from(DoublePrimitiveExample.class).where("id = ?", double_primitive_unset).first().getValue(), DELTA_DOUBLE);
    }

    /** TEST CLASSES **/

    // Float
    public static class FloatBoxedExample extends Record {
        @Indexed
        private Float value;

        public Float getValue() {
            return value;
        }

        public FloatBoxedExample setValue(Float value) {
            this.value = value; return this;
        }
    }

    // float
    public static class FloatPrimitiveExample extends Record {
        @Indexed
        private float value;

        public float getValue() {
            return value;
        }

        public FloatPrimitiveExample setValue(float value) {
            this.value = value;
            return this;
        }
    }

    // Double
    public static class DoubleBoxedExample extends Record {
        @Indexed
        private Double value;

        public Double getValue() {
            return value;
        }

        public DoubleBoxedExample setValue(Double value) {
            this.value = value; return this;
        }
    }

    // double
    public static class DoublePrimitiveExample extends Record {
        @Indexed
        private double value;

        public double getValue() {
            return value;
        }

        public DoublePrimitiveExample setValue(double value) {
            this.value = value;
            return this;
        }
    }

}
