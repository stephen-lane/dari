package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
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
            GZIPOutputStream gzipOutput = new GZIPOutputStream(byteOutput);
            try {
                gzipOutput.write(source);
            } finally {
                gzipOutput.close();
            }

            item.setContentType(contentType);
            Map<String, Object> metaDataMap = new HashMap<String, Object>();
            Map<String, List<String>> httpHeaderMap = new HashMap<String, List<String>>();
            httpHeaderMap.put(CACHE_CONTROL_KEY, Arrays.asList(CACHE_CONTROL_VALUE));
            httpHeaderMap.put("Content-Encoding", Arrays.asList("gzip"));
            metaDataMap.put(AbstractStorageItem.HTTP_HEADERS, httpHeaderMap);
            item.setMetadata(metaDataMap);

            item.setData(new ByteArrayInputStream(byteOutput.toByteArray()));
            item.save();
        }
    }
}
