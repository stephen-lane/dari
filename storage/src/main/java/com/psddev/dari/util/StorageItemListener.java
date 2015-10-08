package com.psddev.dari.util;

import java.io.IOException;

public interface StorageItemListener {

    public void afterSave(StorageItem item) throws IOException;

}
