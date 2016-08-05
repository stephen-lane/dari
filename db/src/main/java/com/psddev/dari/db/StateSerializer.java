package com.psddev.dari.db;

import com.psddev.dari.util.ObjectUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public interface StateSerializer {

    static byte[] serialize(Map<String, Object> values) {
        return ObjectUtils.toJson(values).getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> deserialize(byte[] data) {
        return (Map<String, Object>) ObjectUtils.fromJson(data);
    }
}
