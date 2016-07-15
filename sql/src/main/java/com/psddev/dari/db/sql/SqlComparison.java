package com.psddev.dari.db.sql;

import com.psddev.dari.db.PredicateParser;
import org.jooq.Condition;
import org.jooq.Param;

@FunctionalInterface
interface SqlComparison {

    @SuppressWarnings("unchecked")
    SqlComparison CONTAINS = (join, value) -> join.valueField.like((Param) join.value("%" + value + "%"));

    @SuppressWarnings("unchecked")
    SqlComparison STARTS_WITH = (join, value) -> join.valueField.like((Param) join.value(value + "%"));

    SqlComparison LESS_THAN = (join, value) -> join.valueField.lt(join.value(value));

    SqlComparison LESS_THAN_OR_EQUALS = (join, value) -> join.valueField.le(join.value(value));

    SqlComparison GREATER_THAN = (join, value) -> join.valueField.gt(join.value(value));

    SqlComparison GREATER_THAN_OR_EQUALS = (join, value) -> join.valueField.ge(join.value(value));

    static SqlComparison find(String operator) {
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

    Condition createCondition(SqlJoin join, Object value);
}
