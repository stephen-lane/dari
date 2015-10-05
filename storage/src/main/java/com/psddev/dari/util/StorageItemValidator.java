package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;

public interface StorageItemValidator {
    void validate(File file, String fileContentType) throws IOException;
}
