package com.psddev.dari.db;

public interface ConditionallyValidatable extends Recordable {
    boolean shouldValidate(ObjectField field);
}
