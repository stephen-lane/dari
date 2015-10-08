package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/** Storage item backed by a URL. */
public class UrlStorageItem extends AbstractStorageItem {

    /** Storage name assigned to all instances by default. */
    public static final String DEFAULT_STORAGE = "_url";

    {
        setStorage(DEFAULT_STORAGE);
    }

    // --- AbstractStorageItem support ---

    @Override
    protected InputStream createData() throws IOException {
        return new URL(getPublicUrl()).openStream();
    }

    @Override
    protected void saveData(InputStream data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInStorage() {
        return true;
    }
}
