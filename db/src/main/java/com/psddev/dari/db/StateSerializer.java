package com.psddev.dari.db;

import com.google.common.base.Preconditions;
import com.psddev.dari.util.ObjectUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public interface StateSerializer {

    /**
     * Serializes the given {@code values} map from a {@link State} instance
     * into a byte array that's suitable for storing in a database.
     *
     * @param values Nonnull.
     * @return Nonnull.
     */
    static byte[] serialize(Map<String, Object> values) {
        Preconditions.checkNotNull(values);
        return ObjectUtils.toJson(values).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserializes the given {@code data} byte array from a database into
     * a map that's suitable for use by a {@link State} instance.
     *
     * @param data Nonnull.
     * @return Nonnull.
     */
    static Map<String, Object> deserialize(byte[] data) {
        Preconditions.checkNotNull(data);
        @SuppressWarnings("unchecked")
        Map<String, Object> dataMap = (Map<String, Object>) ObjectUtils.fromJson(data);
        return dataMap;
    }
}
