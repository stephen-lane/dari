package com.psddev.dari.db.sql;

import com.psddev.dari.db.Query;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.List;

final class SqlSubJoin {

    public final SqlQuery sqlQuery;
    public final Table<?> table;
    public final Condition on;

    public static SqlSubJoin create(
            SqlQuery parent,
            Query<?> subQuery,
            boolean forceLeftJoins,
            SqlJoin join,
            boolean in) {

        List<SqlSubJoin> subJoins = parent.subJoins;

        SqlQuery sub = new SqlQuery(
                parent.database,
                subQuery,
                parent.aliasPrefix + "s" + subJoins.size());

        sub.forceLeftJoins = forceLeftJoins;

        SqlSubJoin subJoin = new SqlSubJoin(parent, sub, join, in);

        subJoins.add(subJoin);

        return subJoin;
    }

    private SqlSubJoin(SqlQuery parent, SqlQuery sub, SqlJoin join, boolean in) {
        this.sqlQuery = sub;

        SqlSchema schema = sub.schema;
        String alias = sub.recordTableAlias;
        Field<?> id = DSL.field(DSL.name(alias, schema.recordIdField().getName()), schema.uuidType());

        this.table = sub.initialize(DSL.table(DSL.name(schema.recordTable().getName())).as(alias));
        this.on = in
                ? join.valueField.eq(id)
                : join.valueField.ne(id);

        if (sub.needsDistinct) {
            parent.needsDistinct = true;
        }
    }
}
