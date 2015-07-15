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
import static org.junit.Assert.assertTrue;

/**
 * Created by rhseeger on 7/8/15.
 * Tests dealing with visibility
 */
public class SqlDatabase_Visibility_Test {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabase_Visibility_Test.class);

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


    /** TRASH / ARCHIVED **/
    public static class TrashExample extends Record {}


    /** DRAFT / Pre-Publish **/


    /** DRAFT / Post-Publish **/


    /** WORKFLOWS **/
    public static class WorkflowExample extends Record {
        public WorkflowExample setWorkflow(String workflowState) {
            this.getState().putByPath("cms.workflow.currentState", workflowState);
            return this;
        }
    }

    static WorkflowExample workflowExample_1;

    @BeforeClass
    public static void beforeClass_paginated() {
        workflowExample_1 = new WorkflowExample();
        workflowExample_1.save();
    }


    /** .select(long offset, int limit) **/
    @Test
    public void what_to_test() {

    }

    /** CUSTOM VISIBILITIES **/
    public static class VisibilityExample extends Record {
        @Recordable.Indexed(visibility = true)
        private String vfield;
        public VisibilityExample setVfield(String vfield) { this.vfield = vfield; return this; }
    }

    static VisibilityExample visibilityExample_1, visibilityExample_2, visibilityExample_3;

    @BeforeClass
    public static void beforeClass_visibility() {
        visibilityExample_1 = new VisibilityExample();
        visibilityExample_1.save();
        visibilityExample_2 = new VisibilityExample().setVfield("value 1");
        visibilityExample_2.save();
        visibilityExample_3 = new VisibilityExample().setVfield("value 2");
        visibilityExample_3.save();
    }


    @Test
    public void visibility_default() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class).selectAll();

        assertEquals(Arrays.asList(visibilityExample_1), result);
    }

    @Test
    public void visibility_visible_explicit() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class)
                .where("vfield = missing")
                .selectAll();

        assertEquals(Arrays.asList(visibilityExample_1), result);
    }

    @Test
    public void visibility_invisible() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class)
                .where("vfield = ?", "value 1")
                .selectAll();

        assertEquals(Arrays.asList(visibilityExample_2), result);
    }

    @Test
    public void visibility_both() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class)
                .where("vfield = missing OR vfield = ?", "value 1")
                .selectAll();

        assertEqualsUnordered(Arrays.asList(visibilityExample_1, visibilityExample_2), result);
    }

    @Test // You have to specify visibilty values explicitly; "!= missing" does not work
    // TODO: the [visibiltyField != missing] syntax should really throw an unsupported exception
    public void visibility_notmissing() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class)
                .where("vfield = missing OR vfield != missing")
                .selectAll();

        assertEquals(Arrays.asList(visibilityExample_1), result);
    }

    @Test // You have to specify visibilty values explicitly; "!= missing" does not work
    public void visibility_notmissing_only() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class)
                .where("vfield != missing")
                .selectAll();

        assertTrue(result.isEmpty());
    }

    @Test
    public void visibility_other() {
        List<VisibilityExample> result = Query.from(VisibilityExample.class)
                .where("vfield = ?", "value other")
                .selectAll();

        assertTrue(result.isEmpty());
    }


    /** .resolveInvisible() **/
    /**
     * Some fields are marked as visibility fields and, by having a value, they indicate that the object they belong to
     * is "invisible" as far as normal interactions are concerned. The underlying lazy-load database code automatically
     * converts their objects to null.
     *
     * Note that Query.resolveInvisible doesn't modify the query, it just sets resolveInvisible on all the results returned
     *
     * class Bar extends Record {
     *     @Indexed(visibility = true)
     *     private String whatever;
     *     // setter + getter
     * }
     * class Foo extends Record {
     *     private Bar myBar;
     *     // setter + getter
     * }
     *
     * Bar bar1 = new Bar();
     * bar1.setWhatever("some value");
     * bar1.save();
     *
     * Bar bar2 = new Bar();
     * bar2.setWhatever(null);
     * bar2.save();
     *
     * Foo foo1 = new Foo();
     * foo1.setMyBar(bar1);
     * foo1.save();
     *
     * Foo foo2 = new Foo();
     * foo2.setMyBar(bar2);
     * foo2.save();
     *
     * Query.from(Foo.class).where("id = ?", foo1.getId()).first().getBar(); -> null
     * Query.from(Foo.class).where("id = ?", foo2.getId()).first().getBar(); -> bar2
     */
    public static class ResolveChild extends Record {
        @Recordable.Indexed(visibility = true)
        private String vfield;
        public ResolveChild setVfield(String vfield) { this.vfield = vfield; return this; }
    }
    public static class ResolveParent extends Record {
        private ResolveChild child;
        public ResolveParent setChild(ResolveChild child) { this.child = child; return this; }
        public ResolveChild getChild() { return child; }
    }

    static ResolveChild resolveChild_1, resolveChild_2;
    static ResolveParent resolveParent_1, resolveParent_2;

    @BeforeClass
    public static void beforeClass_resolveInvisible() {
        resolveChild_1 = new ResolveChild().setVfield(null);
        resolveChild_1.save();
        resolveParent_1 = new ResolveParent().setChild(resolveChild_1);
        resolveParent_1.save();
        resolveChild_2 = new ResolveChild().setVfield("hidden");
        resolveChild_2.save();
        resolveParent_2 = new ResolveParent().setChild(resolveChild_2);
        resolveParent_2.save();
    }

    @Test
    public void resolve_visible_noResolve() {
        ResolveParent parent = Query.from(ResolveParent.class).where("id = ?", resolveParent_1.getId()).first();

        assertEquals(resolveChild_1, parent.getChild());
    }

    @Test
    public void resolve_visible_resolve() {
        ResolveParent parent = Query.from(ResolveParent.class).resolveInvisible().where("id = ?", resolveParent_1.getId()).first();

        assertEquals(resolveChild_1, parent.getChild());
    }

    @Test
    public void resolve_invisible_noResolve() {
        ResolveParent parent = Query.from(ResolveParent.class).where("id = ?", resolveParent_2.getId()).first();

        assertEquals(null, parent.getChild());
    }

    @Test
    public void resolve_invisible_resolve() {
        ResolveParent parent = Query.from(ResolveParent.class).resolveInvisible().where("id = ?", resolveParent_2.getId()).first();

        assertEquals(resolveChild_2, parent.getChild());
    }

}
