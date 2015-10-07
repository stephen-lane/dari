package com.psddev.dari.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.fileupload.FileItem;

public class MetadataPreprocessor implements StorageItemPreprocessor {

    @Override
    public void process(StorageItem storageItem, FileItem fileItem) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("originalFileName", fileItem.getName());

        Map<String, List<String>> httpHeaders = new LinkedHashMap<>();
        httpHeaders.put("Cache-Control", Collections.singletonList("public, max-age=31536000"));
        httpHeaders.put("Content-Length", Collections.singletonList(String.valueOf(fileItem.getSize())));
        httpHeaders.put("Content-Type", Collections.singletonList(fileItem.getContentType()));
        metadata.put("http.headers", httpHeaders);

        storageItem.setMetadata(metadata);
    }
}
