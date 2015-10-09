package com.psddev.dari.util;

import java.io.IOException;

/**
 * StorageItemValidator allows an Application to implement custom
 * validation of StorageItems prior to creating them. The validation
 * is performed by {@link StorageItemFilter}.
 */
public interface StorageItemValidator {

    default boolean isSupported(String storage) {
        return true;
    }

    /**
     * Invoked by {@link StorageItemFilter}
     *
     * @param part StorageItemPart containing relevant data
     * @throws IOException
     */
    void validate(StorageItemPart part) throws IOException;
}
