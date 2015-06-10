package com.psddev.dari.util;

import org.apache.commons.codec.DecoderException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DatabaseStorageItem extends AbstractStorageItem {

    /** Storage name assigned to all instances by default. */
    public static final String DEFAULT_STORAGE = "_dbStorage";

    {
        setStorage(DEFAULT_STORAGE);
    }

    private String path;
    private String hexData;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getHexData() {
        return hexData;
    }

    public void setHexData(String hexData) {
        this.hexData = hexData;
    }

    @Override
    protected InputStream createData() throws IOException {

        try {
            byte[] bytes = (byte[]) new org.apache.commons.codec.binary.Hex().decode(getHexData());
            return new ByteArrayInputStream(bytes);
        } catch (DecoderException e) {
            return null;
        }
    }

    @Override
    protected void saveData(InputStream data) throws IOException {

        byte[] source;
        try {
            source = IoUtils.toByteArray(data);
        } finally {
            data.close();
        }

        setHexData(StringUtils.hex(source));
        setPath(DEFAULT_STORAGE + "/" + StringUtils.hex(StringUtils.hash("SHA-256", getHexData())));
    }

    @Override
    public String getPublicUrl() {
        return getProxyStorageItem().getPublicUrl();
    }

    @Override
    public String getSecurePublicUrl() {
        return getProxyStorageItem().getSecurePublicUrl();
    }

    @Override
    public boolean isInStorage() {
        return true;
    }

    private StorageItem getProxyStorageItem() {

        StorageItem proxyStorageItem = StorageItem.Static.create();
        proxyStorageItem.setPath(getPath());
        proxyStorageItem.setContentType(getContentType());

        if (!proxyStorageItem.isInStorage()) {
            try {
                proxyStorageItem.setData(createData());
                proxyStorageItem.save();
            } catch (IOException e) {
                return null;
            }
        }
        return proxyStorageItem;
    }
}
