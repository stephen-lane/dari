package com.psddev.dari.util;

/**
 * StorageItemPostprocessors are executed after an AbstractStorageItem
 * has executed {@link AbstractStorageItem#saveData(InputStream)} if
 * the AbstractStorageItem was created by {@link StorageItemFilter}.
 */
public interface StorageItemPostprocessor {

    /**
     * Invoked from StorageItemFilter to handle additional
     * processing of a StorageItem.
     *
     * @param part StorageItemPart with a StorageItem that has been saved to storage.
     */
    void process(StorageItemPart part);
}
