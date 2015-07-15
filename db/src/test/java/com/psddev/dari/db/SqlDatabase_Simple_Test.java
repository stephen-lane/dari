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

import static org.junit.Assert.assertEquals;
import static com.psddev.dari.db.AssertUtils.assertEqualsUnordered;

/**
 * Created by rhseeger on 7/8/15.
 */
public class SqlDatabase_Simple_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_Simple_Test.class);

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


    /** NO INSTANCES **/
    public static class ConcreteNoInstances extends Record {}

    @Test
    public void simple_concrete_no_instances() {
        List<ConcreteNoInstances> result = Query.from(ConcreteNoInstances.class).selectAll();

        assertEquals(Collections.<ConcreteNoInstances>emptyList(), result);
    }


    /** SINGLE INSTANCE **/
    public static class ConcreteOneInstance extends Record {}

    static ConcreteOneInstance concreteOneInstance;

    @BeforeClass
    public static void beforeClass_single_instance() {
        concreteOneInstance = new ConcreteOneInstance();
        concreteOneInstance.save();
    }

    @Test
    public void simple_concrete_one_instance() {
        List<ConcreteOneInstance> result = Query.from(ConcreteOneInstance.class).selectAll();

        assertEquals(Arrays.asList(concreteOneInstance), result);
    }


    /** MULTIPLE INSTANCES **/
    public static class ConcreteMultipleInstances extends Record {}

    static ConcreteMultipleInstances concreteMultipleInstances_1, concreteMultipleInstances_2;

    @BeforeClass
    public static void beforeClass_multiple_instance() {
        concreteMultipleInstances_1 = new ConcreteMultipleInstances();
        concreteMultipleInstances_1.save();
        concreteMultipleInstances_2 = new ConcreteMultipleInstances();
        concreteMultipleInstances_2.save();
    }

    @Test
    public void simple_concrete_multiple_instance() {
        List<ConcreteMultipleInstances> result = Query.from(ConcreteMultipleInstances.class).selectAll();

        assertEqualsUnordered(Arrays.asList(concreteMultipleInstances_1, concreteMultipleInstances_2), result);
    }


    /** ABSTRACT **/
    public static abstract class AbstractParent extends Record {}
    public static class ConcreteChildOne extends AbstractParent {}
    public static class ConcreteChildTwo extends AbstractParent {}
    public static class ConcreteChildThree extends ConcreteChildTwo {}

    static ConcreteChildOne concreteChildOne;
    static ConcreteChildTwo concreteChildTwo;
    static ConcreteChildThree concreteChildThree;

    @BeforeClass
    public static void beforeClass_abstract_instance() {
        concreteChildOne = new ConcreteChildOne();
        concreteChildOne.save();
        concreteChildTwo = new ConcreteChildTwo();
        concreteChildTwo.save();
        concreteChildThree = new ConcreteChildThree();
        concreteChildThree.save();
    }

    @Test
    public void simple_abstract_all() {
        List<AbstractParent> result = Query.from(AbstractParent.class).selectAll();

        assertEqualsUnordered(Arrays.asList(concreteChildOne, concreteChildTwo, concreteChildThree), result);
    }

    @Test
    public void simple_abstract_child() {
        List<ConcreteChildOne> result = Query.from(ConcreteChildOne.class).selectAll();

        assertEqualsUnordered(Arrays.asList(concreteChildOne), result);
    }

    @Test
    public void simple_abstract_multi_child() {
        List<ConcreteChildTwo> result = Query.from(ConcreteChildTwo.class).selectAll();

        assertEqualsUnordered(Arrays.asList(concreteChildTwo, concreteChildThree), result);
    }

    /** INTERFACE **/
    // TODO: these


    /** EXTENDS CONCRETE **/
    // TODO: these
}
