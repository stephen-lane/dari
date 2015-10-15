package com.psddev.dari.util;

/**
 * StorageItemBeforeSaves are executed before a StorageItem
 * has executed {@link StorageItem#save()} in {@link StorageItemFilter}.
 */
public interface StorageItemBeforeSave {

    /**
     * Invoked by {@link StorageItemFilter}
     *
     * @param part
     *        StorageItemPart containing relevant data
     */
    void beforeSave(StorageItemPart part);
}
