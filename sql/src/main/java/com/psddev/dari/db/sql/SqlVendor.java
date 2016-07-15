package com.psddev.dari.db.sql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import com.psddev.dari.util.IoUtils;

public class SqlVendor {

    /**
     * Returns the path to the resource that contains the SQL statements to
     * be executed during {@link #setUp}. The default implementation returns
     * {@code null} to signal that there's nothing to do.
     *
     * @return May be {@code null}.
     */
    protected String getSetUpResourcePath() {
        return null;
    }

    /**
     * Catches the given {@code error} thrown in {@link #setUp} to be
     * processed in vendor-specific way. Typically, this is used to ignore
     * errors when the vendor doesn't natively support that ability (e.g.
     * {@code CREATE TABLE IF NOT EXISTS}). The default implementation
     * always rethrows the error.
     *
     * @param error Can't be {@code null}.
     */
    protected void catchSetUpError(SQLException error) throws SQLException {
        throw error;
    }

    /**
     * Sets up the given {@code database}. This method should create all the
     * necessary elements, such as tables, that are required for proper
     * operation. The default implementation executes all SQL statements from
     * the resource at {@link #getSetUpResourcePath}, and processes the errors
     * using {@link #catchSetUpError}.
     *
     * @param database Can't be {@code null}.
     */
    public void setUp(AbstractSqlDatabase database) throws IOException, SQLException {
        String resourcePath = getSetUpResourcePath();

        if (resourcePath == null) {
            return;
        }

        InputStream resourceInput = getClass().getClassLoader().getResourceAsStream(resourcePath);

        if (resourceInput == null) {
            throw new IllegalArgumentException(String.format(
                    "Can't find [%s] using ClassLoader#getResourceAsStream!",
                    resourcePath));
        }

        Connection connection = database.openConnection();

        try {
            if (hasTable(connection, AbstractSqlDatabase.RECORD_TABLE)) {
                return;
            }

            for (String ddl : IoUtils.toString(resourceInput, StandardCharsets.UTF_8).trim().split("(?:\r\n?|\n){2,}")) {
                Statement statement = connection.createStatement();

                try {
                    statement.execute(ddl);

                } catch (SQLException error) {
                    catchSetUpError(error);

                } finally {
                    statement.close();
                }
            }

        } finally {
            database.closeConnection(connection);
        }
    }

    protected boolean hasTable(Connection connection, String tableName) throws SQLException {
        return getTables(connection).contains(tableName);
    }

    public Set<String> getTables(Connection connection) throws SQLException {
        Set<String> tableNames = new HashSet<String>();
        String catalog = connection.getCatalog();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet result = meta.getTables(catalog, null, null, null);

        try {
            while (result.next()) {
                String name = result.getString("TABLE_NAME");

                if (name != null) {
                    tableNames.add(name);
                }
            }

        } finally {
            result.close();
        }

        return tableNames;
    }

    protected String rewriteQueryWithLimitClause(String query, int limit, long offset) {
        return String.format("%s LIMIT %d OFFSET %d", query, limit, offset);
    }

    public static class H2 extends SqlVendor {

        @Override
        protected String getSetUpResourcePath() {
            return "h2/schema-12.sql";
        }
    }

    public static class MySQL extends SqlVendor {

        @Override
        protected String getSetUpResourcePath() {
            return "mysql/schema-12.sql";
        }
    }

    public static class PostgreSQL extends SqlVendor {

        @Override
        protected String getSetUpResourcePath() {
            return "postgres/schema-12.sql";
        }

        @Override
        protected boolean hasTable(Connection connection, String tableName) throws SQLException {
            return getTables(connection).contains(tableName.toLowerCase(Locale.ENGLISH));
        }

        @Override
        protected void catchSetUpError(SQLException error) throws SQLException {
            if (!Arrays.asList("42P07").contains(error.getSQLState())) {
                throw error;
            }
        }
    }

    public static class Oracle extends SqlVendor {

        @Override
        protected String rewriteQueryWithLimitClause(String query, int limit, long offset) {
            return String.format("SELECT * FROM "
                    + "    (SELECT a.*, ROWNUM rnum FROM "
                    + "        (%s) a "
                    + "      WHERE ROWNUM <= %d)"
                    + " WHERE rnum  >= %d", query, offset + limit, offset);
        }

        @Override
        public Set<String> getTables(Connection connection) throws SQLException {
            Set<String> tableNames = new HashSet<String>();
            Statement statement = connection.createStatement();

            try {
                ResultSet result = statement.executeQuery("SELECT TABLE_NAME FROM USER_TABLES");

                try {
                    while (result.next()) {
                        String name = result.getString("TABLE_NAME");

                        if (name != null) {
                            tableNames.add(name);
                        }
                    }

                } finally {
                    result.close();
                }

            } finally {
                statement.close();
            }

            return tableNames;
        }
    }
}
