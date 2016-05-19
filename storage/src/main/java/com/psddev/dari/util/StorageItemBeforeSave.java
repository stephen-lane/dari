package com.psddev.dari.util;

import java.io.IOException;

/**
 * StorageItemBeforeSaves are executed before a StorageItem
 * has executed {@code saveData()} in {@link AbstractStorageItem#save()}.
 */
public interface StorageItemBeforeSave {

    /**
     * Invoked by {@link AbstractStorageItem#save()}
     *
     * @param storageItem A StorageItem not yet saved to storage.
     * @param part A StorageItemUploadPart containing upload file and data.
     *
     */
    void beforeSave(StorageItem storageItem, StorageItemUploadPart part) throws IOException;
}
