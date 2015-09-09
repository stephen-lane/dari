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
import java.util.UUID;

import static com.psddev.dari.db.AssertUtils.assertEqualsUnordered;
import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/8/15.
 */
public class SqlDatabase_FieldTypes_UUID_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_FieldTypes_UUID_Test.class);

    @ClassRule
    public static final SqlDatabaseRule res = new SqlDatabaseRule();
    @Rule
    public TestName name = new TestName();

    static UUID uuid_1 = UUID.fromString("0000014f-988a-d86d-afef-dbde75980001");
    static UUID uuid_2 = UUID.fromString("0000014f-988a-d86d-afef-dbde75980002");
    static UUID uuid_3 = UUID.fromString("0000014f-988a-d86d-afef-dbde75980003");
    static UUID uuid_unused = UUID.fromString("0000014f-dead-beef-afef-dbde7598000a");

    static UUIDExample instance_1, instance_2, instance_3, instanceNull;

    @BeforeClass
    public static void beforeClass() {
        instance_1 = new UUIDExample().setValue(uuid_1);
        instance_1.save();
        instance_2 = new UUIDExample().setValue(uuid_2);
        instance_2.save();
        instance_3 = new UUIDExample().setValue(uuid_3);
        instance_3.save();
        instanceNull = new UUIDExample().setValue(null);
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
        UUIDExample result = Query.from(UUIDExample.class).where("value = ?", uuid_1).first();

        assertEquals(instance_1, result);
    }

    @Test
    public void test_equals_null() {
        UUIDExample result = Query.from(UUIDExample.class).where("value = missing").first();

        assertEquals(instanceNull, result);
    }

    @Test
    public void test_equals_none() {
        UUIDExample result = Query.from(UUIDExample.class).where("value = ?", uuid_unused).first();

        assertEquals(null, result);
    }

    /** OTHER COMPARISONS **/
    @Test
    public void test_not_equals() {
        List<UUIDExample> result = Query.from(UUIDExample.class).where("value != ?", uuid_1).selectAll();

        assertEqualsUnordered(Arrays.asList(instance_2, instance_3, instanceNull), result);
    }


    /** SORTING **/
    /* null sorts as lowest value */
    @Test
    public void test_sort_ascending() {
        List<UUIDExample> result = Query.from(UUIDExample.class).sortAscending("value").selectAll();

        assertEquals(Arrays.asList(instanceNull, instance_1, instance_2, instance_3), result);
    }

    @Test
    public void test_sort_descending() {
        List<UUIDExample> result = Query.from(UUIDExample.class).sortDescending("value").selectAll();

        assertEquals(Arrays.asList(instance_3, instance_2, instance_1, instanceNull), result);
    }

    /** VALUE **/
    @Test
    public void test_value() {
        UUIDExample result = Query.from(UUIDExample.class).where("id = ?", instance_1).first();

        assertEquals(uuid_1, result.getValue());
    }

    @Test
    public void test_value_null_correct() {
        UUIDExample result = Query.from(UUIDExample.class).where("id = ?", instanceNull).first();

        assertEquals(null, result.getValue());
    }


    /** TEST CLASSES **/
    public static class UUIDExample extends Record {
        @Indexed
        private UUID value;

        public UUID getValue() {
            return value;
        }

        public UUIDExample setValue(UUID value) {
            this.value = value; return this;
        }
    }
    
}
