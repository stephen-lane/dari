package com.psddev.dari.util;

import java.io.InputStream;

import org.apache.commons.fileupload.FileItem;

/**
 * StorageItemPreprocessors are executed before a StorageItem
 * has executed {@link StorageItem#save()} in {@link StorageItemFilter}.
 */
public interface StorageItemPreprocessor {

    /**
     * Invoked by StorageItemListener
     *
     * @param storageItem StorageItem to be processed
     * @param fileItem FileItem from the multipart request
     */
    void process(StorageItem storageItem, FileItem fileItem);
}
