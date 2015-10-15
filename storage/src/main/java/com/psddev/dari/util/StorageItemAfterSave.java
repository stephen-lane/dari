package com.psddev.dari.util;

/**
 * StorageItemAfterSaves are executed after a StorageItem
 * has executed {@link StorageItem#save()} in {@link StorageItemFilter}.
 */
public interface StorageItemAfterSave {

    /**
     * Invoked from {@link StorageItemFilter}
     *
     * @param part
     *        StorageItemPart with a StorageItem that has been saved to storage.
     */
    void afterSave(StorageItemPart part);
}
