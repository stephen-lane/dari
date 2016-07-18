package com.psddev.dari.db.h2;

import com.psddev.dari.db.SqlVendor;
import com.psddev.dari.db.sql.AbstractSqlDatabase;
import com.psddev.dari.db.sql.SqlSchema;
import com.psddev.dari.util.Lazy;
import org.jooq.SQLDialect;

public class H2Database extends AbstractSqlDatabase {

    private final transient Lazy<H2Schema> schema = new Lazy<H2Schema>() {

        @Override
        protected H2Schema create() {
            return new H2Schema(H2Database.this);
        }
    };

    @Override
    protected SQLDialect dialect() {
        return SQLDialect.H2;
    }

    @Override
    protected SqlSchema schema() {
        return schema.get();
    }

    @Override
    public SqlVendor getMetricVendor() {
        return new SqlVendor.H2();
    }
}
