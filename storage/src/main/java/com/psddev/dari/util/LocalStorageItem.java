package com.psddev.dari.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/** Item stored in the local file system. */
public class LocalStorageItem extends AbstractStorageItem {

    /** Setting key for root path. */
    public static final String ROOT_PATH_SETTING = "rootPath";

    private transient String rootPath;

    /** Returns the root path. */
    public String getRootPath() {
        return rootPath;
    }

    /** Sets the root path. */
    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    // --- AbstractStorageItem support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        super.initialize(settingsKey, settings);

        setRootPath(ObjectUtils.to(String.class, settings.get(ROOT_PATH_SETTING)));
        if (ObjectUtils.isBlank(getRootPath())) {
            throw new SettingsException(settingsKey + "/" + ROOT_PATH_SETTING, "No root path!");
        }
    }

    @Override
    protected InputStream createData() throws IOException {
        return new FileInputStream(new File(getRootPath() + "/" + getPath()));
    }

    @Override
    protected void saveData(InputStream data) throws IOException {
        File file = new File(getRootPath() + "/" + getPath());
        IoUtils.createParentDirectories(file);
        FileOutputStream output = null;

        try {
            output = new FileOutputStream(file);
            int bytesRead;
            byte[] buffer = new byte[4096];
            while ((bytesRead = data.read(buffer)) > 0) {
                output.write(buffer, 0, bytesRead);
            }

        } finally {
            if (output != null) {
                output.close();
            }
        }
    }

    @Override
    public boolean isInStorage() {
        return new File(getRootPath() + "/" + getPath()).exists();
    }
}
