package com.psddev.dari.util;

/**
 * StorageItemAfterSaves are executed after a StorageItem
 * has executed {@link StorageItem#save()} in {@link StorageItemFilter}.
 */
public interface StorageItemAfterSave {

    /**
     * Invoked from {@link StorageItemFilter}
     *
     * @param storageItem
     *        A StorageItem after it has been saved to storage.
     */
    void afterSave(StorageItem storageItem);
}
