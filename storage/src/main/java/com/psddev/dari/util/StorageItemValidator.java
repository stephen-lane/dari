package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;

/**
 * StorageItemValidator allows an Application to implement custom
 * validation of StorageItems prior to creating them. The validation
 * is performed by {@link StorageItemFilter}.
 */
public interface StorageItemValidator {
    void validate(File file, String fileContentType) throws IOException;
}
