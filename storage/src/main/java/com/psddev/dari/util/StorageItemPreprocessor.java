package com.psddev.dari.util;

import org.apache.commons.fileupload.FileItem;

public interface StorageItemPreprocessor {

    void process(StorageItem storageItem, FileItem fileItem);
}
