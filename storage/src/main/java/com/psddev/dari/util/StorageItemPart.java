package com.psddev.dari.util;

import java.io.File;

import org.apache.commons.fileupload.FileItem;

/**
 * Common wrapper class used by {@link StorageItemFilter}
 * processors and validators.
 */
public class StorageItemPart {

    private String name;
    private FileItem fileItem;
    private File file;
    private StorageItem storageItem;
    private String storageName;
    private String contentType;
    private long size;

    public String getContentType() {
        if (contentType == null) {
            contentType = getFileItem() != null ? getFileItem().getContentType() : null;
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

    public long getSize() {
        if (size == 0) {
            size = getFileItem() != null ? getFileItem().getSize() : 0;
        }
        return size;
    }

    public String getName() {
        if (name == null) {
            name = getFileItem() != null ? getFileItem().getName() : null;
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(long size) {
        this.size = size;
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
}
