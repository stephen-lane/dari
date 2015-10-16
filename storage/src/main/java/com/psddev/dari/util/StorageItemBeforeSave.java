package com.psddev.dari.util;

import java.io.IOException;

/**
 * StorageItemBeforeSaves are executed before a StorageItem
 * has executed {@link StorageItem#save()} in {@link StorageItemFilter}.
 */
public interface StorageItemBeforeSave {

    /**
     * Invoked by {@link StorageItemFilter}
     *
     * @param storageItem
     *        A StorageItem not yet saved to storage.
     */
    void beforeSave(StorageItem storageItem) throws IOException;
}
