package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CdnCache extends PullThroughCache<String, Map<CdnContext, Map<String, StorageItem>>> {

    static final String CACHE_CONTROL_KEY = "Cache-Control";
    static final String CACHE_CONTROL_VALUE = "public, max-age=31536000";

    private static final Pattern CSS_URL_PATTERN = Pattern.compile("(?i)url\\((['\"]?)([^)?#]*)([?#][^)]+)?\\1\\)");

    protected String createPath(String contentType, String pathPrefix, String extension) {
        return extension != null ? pathPrefix + "." + extension : pathPrefix;
    }

    protected void saveItem(String contentType, StorageItem item, byte[] source) throws IOException {
        item.setContentType(contentType);

        Map<String, Object> metaDataMap = new HashMap<String, Object>();
        Map<String, List<String>> httpHeaderMap = new HashMap<String, List<String>>();
        httpHeaderMap.put(CACHE_CONTROL_KEY, Arrays.asList(CACHE_CONTROL_VALUE));
        metaDataMap.put(AbstractStorageItem.HTTP_HEADERS, httpHeaderMap);
        item.setMetadata(metaDataMap);

        item.setData(new ByteArrayInputStream(source));
        item.save();
    }

    @Override
    protected Map<CdnContext, Map<String, StorageItem>> produce(final String storage) {
        return new PullThroughCache<CdnContext, Map<String, StorageItem>>() {

            @Override
            protected Map<String, StorageItem> produce(CdnContext cdnContext) {
                return new PullThroughCache<String, StorageItem>() {

                    @Override
                    protected boolean isExpired(String servletPath, Date lastProduceDate) {
                        try {
                            return cdnContext.getLastModified(servletPath) > lastProduceDate.getTime();

                        } catch (IOException error) {
                            return false;
                        }
                    }

                    @Override
                    protected StorageItem produce(String servletPath) throws IOException, NoSuchAlgorithmException, URISyntaxException {
                        byte[] source;

                        try (InputStream sourceInput = cdnContext.open(servletPath)) {
                            source = IoUtils.toByteArray(sourceInput);

                        } catch (IOException error) {
                            return null;
                        }

                        // path -> resource/context/path
                        String contentType = ObjectUtils.getContentType(servletPath);
                        String pathPrefix = "resource"
                                + StringUtils.ensureSurrounding(cdnContext.getPathPrefix(), "/")
                                + StringUtils.removeStart(servletPath, "/");

                        String path;

                        MessageDigest md5 = MessageDigest.getInstance("MD5");
                        md5.update((byte) 16);
                        String hash = StringUtils.hex(md5.digest(source));

                        // name.ext -> createPath(name.hash, ext)
                        int dotAt = pathPrefix.lastIndexOf('.');
                        if (dotAt > -1) {
                            String extension = pathPrefix.substring(dotAt + 1);
                            pathPrefix = pathPrefix.substring(0, dotAt) + "." + hash;
                            path = createPath(contentType, pathPrefix, extension);

                            // name.ext -> createPath(name-hash, null)
                        } else {
                            pathPrefix += "-" + hash;
                            path = createPath(contentType, pathPrefix, null);
                        }

                        // Look into CSS files and change all the URLs.
                        if ("text/css".equals(contentType)) {
                            String css = new String(source, StandardCharsets.UTF_8);
                            StringBuilder newCssBuilder = new StringBuilder();
                            Matcher urlMatcher = CSS_URL_PATTERN.matcher(css);
                            int previous = 0;
                            String childPath;
                            URI childUri;
                            StorageItem childItem;
                            String extra;
                            int slashAt;

                            while (urlMatcher.find()) {
                                newCssBuilder.append(css.substring(previous, urlMatcher.start()));
                                previous = urlMatcher.end();
                                childPath = urlMatcher.group(2);
                                extra = urlMatcher.group(3);

                                newCssBuilder.append("url(");

                                if (childPath.length() == 0) {
                                    newCssBuilder.append("''");

                                } else if (childPath.startsWith("data:")
                                        || childPath.endsWith(".htc")) {
                                    newCssBuilder.append(childPath);

                                } else {
                                    childUri = new URI(servletPath).resolve(childPath);

                                    if (childUri.isAbsolute()) {
                                        newCssBuilder.append(childUri);

                                    } else {
                                        childItem = get(childUri.toString());
                                        for (slashAt = 1; (slashAt = path.indexOf('/', slashAt)) > -1; ++slashAt) {
                                            newCssBuilder.append("../");
                                        }
                                        newCssBuilder.append(childItem != null ? childItem.getPath() : childPath);
                                    }

                                    if (extra != null) {
                                        newCssBuilder.append(extra);
                                    }
                                }

                                newCssBuilder.append(')');
                            }

                            newCssBuilder.append(css.substring(previous, css.length()));
                            source = newCssBuilder.toString().getBytes(StandardCharsets.UTF_8);
                        }

                        StorageItem item = StorageItem.Static.createIn(storage);
                        item.setPath(path);
                        if (!item.isInStorage()) {
                            saveItem(contentType, item, source);
                        }

                        return item;
                    }
                };
            }
        };
    }
}
