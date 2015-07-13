package com.psddev.dari.db;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rhseeger on 7/13/15.
 * Sets up an in-memory MySQL database instance for use in testing
 *
 *     @ClassRule
 *     public static final SqlDatabaseRule res = new SqlDatabaseRule();
 */
public class SqlDatabaseRule extends ExternalResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabaseRule.class);

    static TestDatabase testDb;

    protected void before() {
        testDb = DatabaseTestUtils.getMySQLTestDatabase();
        Database db = testDb.get();
        Database.Static.overrideDefault(db);
        LOGGER.info("Running tests against [" + db.getClass() + " / " + db.getName() + "] database.");
    }
    protected void after() {
        Database.Static.restoreDefault();
        testDb.close();
    }

}
