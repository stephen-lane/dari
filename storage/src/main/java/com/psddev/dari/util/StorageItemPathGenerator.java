package com.psddev.dari.util;

import java.util.UUID;

import com.google.common.base.Preconditions;

/**
 * Default implementation of path generation for
 * StorageItem uploads.
 */
public interface StorageItemPathGenerator {

    default boolean isSupported(String storageName) {
        return true;
    }

    default String createPath(String fullFileName) {
        Preconditions.checkArgument(!StringUtils.isBlank(fullFileName));

        String path = "";

        String idString = UUID.randomUUID().toString().replace("-", "");

        path += idString.substring(0, 2);
        path += '/';
        path += idString.substring(2, 4);
        path += '/';
        path += idString.substring(4);
        path += '/';

        String extension = "";
        String fileName = "";

        int lastDotAt = fullFileName.indexOf('.');

        if (lastDotAt > -1) {
            extension = fullFileName.substring(lastDotAt);
            fileName = fullFileName.substring(0, lastDotAt);
        }

        if (ObjectUtils.isBlank(fileName)) {
            fileName = UUID.randomUUID().toString().replace("-", "");
        }

        path += StringUtils.toNormalized(fileName);
        path += extension;

        return path;
    }
}
