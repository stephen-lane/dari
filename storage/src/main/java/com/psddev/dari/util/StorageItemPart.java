package com.psddev.dari.util;

import java.io.File;

import org.apache.commons.fileupload.FileItem;

/**
 * Common wrapper class for {@link StorageItemFilter}.
 */
public class StorageItemPart {

    private FileItem fileItem;
    private File file;
    private StorageItem storageItem;
    private String storageName;
    private String contentType;

    public String getContentType() {
        if (contentType == null && getFileItem() != null) {
            contentType = getFileItem().getContentType();
        }
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

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

    public String getName() {
        return getFileItem() != null ? getFileItem().getName() : null;
    }

    public long getSize() {
        return getFileItem() != null ? getFileItem().getSize() : 0;
    }
}
