package com.psddev.dari.util;

import org.apache.commons.fileupload.FileItem;

/**
 * Common wrapper class for {@link StorageItemFilter}.
 */
public class StorageItemPart {

    private FileItem fileItem;
    private StorageItem storageItem;
    private String storageName;

    public FileItem getFileItem() {
        return fileItem;
    }

    public void setFileItem(FileItem fileItem) {
        this.fileItem = fileItem;
    }

    public StorageItem getStorageItem() {
        return storageItem;
    }

    public void setStorageItem(StorageItem storageItem) {
        this.storageItem = storageItem;
    }

    public String getStorageName() {
        return storageName;
    }

    public void setStorageName(String storageName) {
        this.storageName = storageName;
    }

    public String getContentType() {
        return getFileItem() != null ? getFileItem().getContentType() : null;
    }

    public String getName() {
        return getFileItem() != null ? getFileItem().getName() : null;
    }

    public long getSize() {
        return getFileItem() != null ? getFileItem().getSize() : 0;
    }
}
