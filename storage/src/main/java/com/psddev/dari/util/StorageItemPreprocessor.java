package com.psddev.dari.util;

/**
 * StorageItemPreprocessors are executed before a StorageItem
 * has executed {@link StorageItem#save()} in {@link StorageItemFilter}.
 */
public interface StorageItemPreprocessor {

    /**
     * Invoked by {@link StorageItemListener}
     *
     * @param part StorageItemPart containing relevant data
     */
    void process(StorageItemPart part);
}
