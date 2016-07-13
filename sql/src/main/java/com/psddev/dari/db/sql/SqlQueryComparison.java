package com.psddev.dari.db.sql;

import com.psddev.dari.db.PredicateParser;
import org.jooq.Condition;
import org.jooq.Field;

@FunctionalInterface
interface SqlQueryComparison {

    SqlQueryComparison CONTAINS = (field, value) -> field.like("%" + value + "%");

    SqlQueryComparison STARTS_WITH = (field, value) -> field.like(value + "%");

    SqlQueryComparison LESS_THAN = Field::lt;

    SqlQueryComparison LESS_THAN_OR_EQUALS = Field::le;

    SqlQueryComparison GREATER_THAN = Field::gt;

    SqlQueryComparison GREATER_THAN_OR_EQUALS = Field::ge;

    static SqlQueryComparison find(String operator) {
        switch (operator) {
            case PredicateParser.CONTAINS_OPERATOR :
                return CONTAINS;

            case PredicateParser.STARTS_WITH_OPERATOR :
                return STARTS_WITH;

            case PredicateParser.LESS_THAN_OPERATOR :
                return LESS_THAN;

            case PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR :
                return LESS_THAN_OR_EQUALS;

            case PredicateParser.GREATER_THAN_OPERATOR :
                return GREATER_THAN;

            case PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR :
                return GREATER_THAN_OR_EQUALS;

            default :
                return null;
        }
    }

    Condition createCondition(Field<Object> field, Object value);
}
