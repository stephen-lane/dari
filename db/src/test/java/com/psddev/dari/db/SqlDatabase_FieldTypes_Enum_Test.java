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
public class SqlDatabase_FieldTypes_Enum_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_FieldTypes_Enum_Test.class);

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();


    static EnumExample instance_1, instance_2, instance_3, instanceNull;

    @BeforeClass
    public static void beforeClass() {
        instance_1 = new EnumExample().setValue(EnumValue.One);
        instance_1.save();
        instance_2 = new EnumExample().setValue(EnumValue.Two);
        instance_2.save();
        instance_3 = new EnumExample().setValue(EnumValue.Three);
        instance_3.save();
        instanceNull = new EnumExample().setValue(null);
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
        EnumExample result = Query.from(EnumExample.class).where("value = ?", EnumValue.One).first();

        assertEquals(instance_1, result);
    }

    @Test
    public void test_equals_null() {
        EnumExample result = Query.from(EnumExample.class).where("value = missing").first();

        assertEquals(instanceNull, result);
    }

    @Test
    public void test_equals_none() {
        EnumExample result = Query.from(EnumExample.class).where("value = ?", EnumValue.Unused).first();

        assertEquals(null, result);
    }

    /** OTHER COMPARISONS **/
    @Test
    public void test_not_equals() {
        List<EnumExample> result = Query.from(EnumExample.class).where("value != ?", EnumValue.One).selectAll();

        assertEqualsUnordered(Arrays.asList(instance_2, instance_3, instanceNull), result);
    }

    /** VALUE **/
    @Test
    public void test_value() {
        EnumExample result = Query.from(EnumExample.class).where("id = ?", instance_1).first();

        assertEquals(EnumValue.One, result.getValue());
    }

    @Test
    public void test_value_null_correct() {
        EnumExample result = Query.from(EnumExample.class).where("id = ?", instanceNull).first();

        assertEquals(null, result.getValue());
    }


    /** TEST CLASSES **/
    public static class EnumExample extends Record {
        @Indexed
        private EnumValue value;

        public EnumValue getValue() {
            return value;
        }

        public EnumExample setValue(EnumValue value) {
            this.value = value; return this;
        }
    }

    public enum EnumValue {
        One(), Two(), Three(), Unused();
    }
    
}
