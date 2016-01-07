package com.psddev.dari.db;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Set;

import java8.util.stream.Collectors;
import java8.util.stream.StreamSupport;

/**
 * Object that can contain fields and indexes.
 */
public interface ObjectStruct {

    /**
     * Finds all fields that are indexed in the given {@code struct}.
     *
     * @param struct
     *        Can't be {@code null}.
     *
     * @return Never {@code null}.
     */
    static List<ObjectField> findIndexedFields(ObjectStruct struct) {
        Preconditions.checkNotNull(struct);

        Set<String> indexed = StreamSupport.stream(struct.getIndexes())
                .flatMap(index -> StreamSupport.stream(index.getFields()))
                .collect(Collectors.toSet());

        List<ObjectField> fields = struct.getFields();

        return StreamSupport.stream(fields).filter(field -> indexed.contains(field.getInternalName())).collect(Collectors.toList());
    }

    /**
     * Returns the environment that owns this struct.
     *
     * @return Never {@code null}.
     */
    DatabaseEnvironment getEnvironment();

    /**
     * Returns a list of all fields.
     *
     * @return Never {@code null}.
     */
    List<ObjectField> getFields();

    /**
     * Returns the field with the given {@code name}.
     *
     * @param name
     *        If {@code null}, returns {@code null}.
     *
     * @return May be {@code null}.
     */
    ObjectField getField(String name);

    /**
     * Sets the list of all fields.
     *
     * @param fields
     *        {@code null} to clear.
     */
    void setFields(List<ObjectField> fields);

    /**
     * Returns a list of all indexes.
     *
     * @return Never {@code null}.
     */
    List<ObjectIndex> getIndexes();

    /**
     * Returns the index with the given {@code name}.
     *
     * @param name
     *        If {@code null}, returns {@code null}.
     *
     * @return May be {@code null}.
     */
    ObjectIndex getIndex(String name);

    /**
     * Sets the list of all indexes.
     *
     * @param indexes
     *        {@code null} to clear.
     */
    void setIndexes(List<ObjectIndex> indexes);

    /**
     * {@link ObjectStruct} utility methods.
     *
     * @deprecated Use {@link ObjectStruct} instead.
     */
    @Deprecated
    final class Static {

        /**
         * Returns all fields that are indexed in the given {@code struct}.
         *
         * @param struct
         *        Can't be {@code null}.
         *
         * @return Never {@code null}.
         *
         * @deprecated Use {@link ObjectStruct#findIndexedFields(ObjectStruct)}
         *             instead.
         */
        @Deprecated
        public static List<ObjectField> findIndexedFields(ObjectStruct struct) {
            return ObjectStruct.findIndexedFields(struct);
        }
    }
}
