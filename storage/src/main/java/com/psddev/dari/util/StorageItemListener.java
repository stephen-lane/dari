package com.psddev.dari.util;

import java.io.IOException;

//TODO: deprecate and replace with StorageItemAfterSave in dari 3.2
public interface StorageItemListener {

    public void afterSave(StorageItem item) throws IOException;

}
