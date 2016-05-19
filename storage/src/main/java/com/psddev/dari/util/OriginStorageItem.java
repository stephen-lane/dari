package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

public class OriginStorageItem implements StorageItem {

    private StorageItem item;

    public OriginStorageItem(StorageItem item) {
        this.item = item;
    }

    @Override
    public String getStorage() {
        return item.getStorage();
    }

    @Override
    public void setStorage(String storage) {
        item.setStorage(storage);
    }

    @Override
    public String getPath() {
        return item.getPath();
    }

    @Override
    public void setPath(String path) {
        item.setPath(path);
    }

    @Override
    public String getContentType() {
        return item.getContentType();
    }

    @Override
    public void setContentType(String contentType) {
        item.setContentType(contentType);
    }

    @Override
    public Map<String, Object> getMetadata() {
        return item.getMetadata();
    }

    @Override
    public void setMetadata(Map<String, Object> metadata) {
        item.setMetadata(metadata);
    }

    @Override
    public InputStream getData() throws IOException {
        return item.getData();
    }

    @Override
    public void setData(InputStream data) {
        item.setData(data);
    }

    @Override
    public URL getUrl() {
        return null;
    }

    @Override
    public String getSecurePublicUrl() {
        return item.getSecurePublicUrl();
    }

    @Override
    public void save() throws IOException {
        item.save();
    }

    @Override
    public String toString() {
        return item.toString();
    }

    @Override
    public boolean isInStorage() {
        return item.isInStorage();
    }

    @Override
    public String getPublicUrl() {
        String url = null;
        if (item instanceof StorageItemOriginUrl) {
            url = ((StorageItemOriginUrl) item).getOriginUrl();
        }

        if (url == null) {
            url = item.getPublicUrl();
        }

        return url;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        throw new UnsupportedOperationException();
    }
}
