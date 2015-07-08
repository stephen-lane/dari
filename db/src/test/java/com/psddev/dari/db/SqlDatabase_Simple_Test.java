package com.psddev.dari.db;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Created by rhseeger on 7/8/15.
 * TODO: Make creating instances and saving their IDs easier
 */
public class SqlDatabase_Simple_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_Simple_Test.class);

    static UUID uuid_ConcreteOneInstance,
            uuid_ConcreteMultipleInstances_1, uuid_ConcreteMultipleInstances_2;
    static TestDatabase testDb;

    @BeforeClass
    public static void beforeClass() {
        testDb = DatabaseTestUtils.getMySQLTestDatabase();
        Database db = testDb.get();
        Database.Static.overrideDefault(db);
        LOGGER.info("Running tests against [" + db.getClass() + " / " + db.getName() + "] database.");

        ConcreteOneInstance concreteOneInstance = new ConcreteOneInstance();
        concreteOneInstance.save();
        uuid_ConcreteOneInstance = concreteOneInstance.getId();

        ConcreteMultipleInstances concreteMultipleInstances;
        concreteMultipleInstances = new ConcreteMultipleInstances();
        concreteMultipleInstances.save();
        uuid_ConcreteMultipleInstances_1 = concreteMultipleInstances.getId();
        concreteMultipleInstances = new ConcreteMultipleInstances();
        concreteMultipleInstances.save();
        uuid_ConcreteMultipleInstances_2 = concreteMultipleInstances.getId();

    }

    @AfterClass
    public static void afterClass() {
        Database.Static.restoreDefault();
        testDb.close();
    }

    @Before
    public void before() {
    }

    @After
    public void after() {
    }

    @Test
    public void simple_concrete_one_instance() {
        List<ConcreteOneInstance> result = Query.from(ConcreteOneInstance.class).selectAll();

        assertEquals(Arrays.asList(new UUID[]{uuid_ConcreteOneInstance}), result.stream().map(instance -> instance.getId()).collect(Collectors.toList()));
    }

    @Test
    public void simple_concrete_no_instances() {
        List<ConcreteNoInstances> result = Query.from(ConcreteNoInstances.class).selectAll();

        assertEquals(Collections.<ConcreteNoInstances>emptyList(), result);
    }

    @Test
    public void simple_concrete_multiple_instance() {
        List<ConcreteMultipleInstances> result = Query.from(ConcreteMultipleInstances.class).selectAll();

        List<UUID> expect = Arrays.asList(new UUID[]{
                uuid_ConcreteMultipleInstances_1,
                uuid_ConcreteMultipleInstances_2
        });
        assertEquals(expect, result.stream().map(instance -> instance.getId()).collect(Collectors.toList()));

    }


    /** CLASSES FOR TESTING **/
    public static class ConcreteOneInstance extends Record {}
    public static class ConcreteMultipleInstances extends Record {}
    public static class ConcreteNoInstances extends Record {}
}
