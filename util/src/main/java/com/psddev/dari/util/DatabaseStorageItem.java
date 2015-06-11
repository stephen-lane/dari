package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DatabaseStorageItem extends AbstractStorageItem {

    /** Storage name assigned to all instances by default. */
    public static final String DEFAULT_STORAGE = "_dbStorage";

    {
        setStorage(DEFAULT_STORAGE);
    }

    private String binaryData;

    public String getBinaryData() {
        return binaryData;
    }

    public void setBinaryData(String binaryData) {
        this.binaryData = binaryData;
    }

    @Override
    protected InputStream createData() throws IOException {

        return new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(getBinaryData())));
    }

    @Override
    protected void saveData(InputStream data) throws IOException {

        byte[] source;
        try {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOutput = new GZIPOutputStream(byteOutput)) {

                gzipOutput.write(IoUtils.toByteArray(data));
            }
            source = byteOutput.toByteArray();
        } finally {
            data.close();
        }

        setBinaryData(Base64.getEncoder().encodeToString(source));
        setPath(DEFAULT_STORAGE + "/" + StringUtils.encodeUri(Base64.getEncoder().encodeToString(StringUtils.hash("SHA-256", getBinaryData()))));
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
        if (DEFAULT_STORAGE.equals(proxyStorageItem.getStorage())) {
            return null;
        }
        proxyStorageItem.setPath(getPath());
        proxyStorageItem.setContentType(getContentType());
        proxyStorageItem.setMetadata(getMetadata());

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
