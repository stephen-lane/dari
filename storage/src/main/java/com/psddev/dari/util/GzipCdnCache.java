package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

class GzipCdnCache extends CdnCache {

    @Override
    protected String createPath(String contentType, String pathPrefix, String extension) {
        if (!contentType.startsWith("text/")) {
            return super.createPath(contentType, pathPrefix, extension);

        } else {
            return extension != null ? pathPrefix + ".gz." + extension : pathPrefix + "-gz";
        }
    }

    @Override
    protected void saveItem(String contentType, StorageItem item, byte[] source) throws IOException {
        if (!contentType.startsWith("text/")) {
            super.saveItem(contentType, item, source);

        } else {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();

            try (GZIPOutputStream gzipOutput = new GZIPOutputStream(byteOutput)) {
                gzipOutput.write(source);
            }

            item.setContentType(contentType);
            Map<String, Object> metaDataMap = new HashMap<>();
            Map<String, List<String>> httpHeaderMap = new HashMap<>();
            httpHeaderMap.put(CACHE_CONTROL_KEY, Collections.singletonList(CACHE_CONTROL_VALUE));
            httpHeaderMap.put("Content-Encoding", Collections.singletonList("gzip"));
            metaDataMap.put(AbstractStorageItem.HTTP_HEADERS, httpHeaderMap);
            item.setMetadata(metaDataMap);

            item.setData(new ByteArrayInputStream(byteOutput.toByteArray()));
            item.save();
        }
    }
}
