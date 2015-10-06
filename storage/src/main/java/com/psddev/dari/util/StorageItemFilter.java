package com.psddev.dari.util;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import com.google.common.base.Preconditions;

/**
 * For creating {@link StorageItem}(s) from a {@link MultipartRequest}
 */
public class StorageItemFilter extends AbstractFilter {

    private static final String UPLOAD_PATH = "/_dari/upload";
    private static final String FILE_PARAM = "fileParam";
    private static final String STORAGE_PARAM = "storageName";

    /**
     * Intercepts requests to {@code UPLOAD_PATH},
     * creates a {@link StorageItem} and returns the StorageItem as json.
     *
     * @param request  Can't be {@code null}.
     * @param response Can't be {@code null}.
     * @param chain    Can't be {@code null}.
     * @throws Exception
     */
    @Override
    protected void doRequest(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws Exception {

        if (request.getRequestURI().equals(UPLOAD_PATH)) {
            String fileParam = request.getParameter(FILE_PARAM);
            String storageName = request.getParameter(STORAGE_PARAM);
            StorageItem storageItem = StorageItemFilter.getParameter(request, fileParam, storageName);

            WebPageContext page = new WebPageContext((ServletContext) null, request, response);
            response.setContentType("application/json");
            page.write(ObjectUtils.toJson(storageItem));
            return;
        }

        chain.doFilter(request, response);
    }

    /**
     * Creates {@link StorageItem} from a request and request parameter.
     *
     * @param request     Can't be {@code null}.
     * @param paramName   The parameter name for the file input. Can't be {@code null} or blank.
     * @param storageName Optionally accepts a storageName, will default to using {@code StorageItem.DEFAULT_STORAGE_SETTING}
     * @return the created {@link StorageItem}
     */
    public static StorageItem getParameter(HttpServletRequest request, String paramName, String storageName) throws IOException {
        Preconditions.checkNotNull(request);
        Preconditions.checkArgument(!StringUtils.isBlank(paramName));

        String storageItemJson = request.getParameter(paramName);

        StorageItem storageItem = null;

        if (storageItemJson != null && !storageItemJson.equals("file")) {
            storageItem = createStorageItem(storageItemJson);
        } else {

            MultipartRequest mpRequest = MultipartRequestFilter.Static.getInstance(request);

            if (mpRequest != null) {
                FileItem fileItem = mpRequest.getFileItem(paramName);
                if (fileItem != null) {
                    storageItem = createStorageItem(fileItem, storageName);
                }
            }
        }

        return storageItem;
    }

    private static StorageItem createStorageItem(FileItem fileItem, String storageName) throws IOException {

        final String finalStorageName;
        if (StringUtils.isBlank(storageName)) {
            finalStorageName = StorageItem.DEFAULT_STORAGE_SETTING;
        } else {
            finalStorageName = storageName;
        }

        String storageSetting = Preconditions.checkNotNull(Settings.get(String.class, finalStorageName),
                "Storage setting with key [" + finalStorageName + "] not found in application settings.");

        File file;
        try {
            file = File.createTempFile("cms.", ".tmp");
            fileItem.write(file);
        } catch (Exception e) {
            throw new IOException("Unable to write [" + (fileItem != null ? fileItem.getName() : "fileItem") + "] to temporary file.", e);
        }

        String fileName = Preconditions.checkNotNull(fileItem.getName());

        String fileContentType = fileItem.getContentType();
        long fileSize = fileItem.getSize();

        Preconditions.checkState(fileSize > 0,
                "File [" + fileName + "] is empty");

        ClassFinder.findConcreteClasses(StorageItemValidator.class)
                .forEach(c -> {
                    try {
                        StorageItemValidator validator = TypeDefinition.getInstance(c).newInstance();
                        if (validator.isSupported(finalStorageName)) {
                            validator.validate(file, fileContentType);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        StorageItem storageItem = StorageItem.Static.createIn(storageSetting);
        storageItem.setData(new FileInputStream(file));
        storageItem.setContentType(fileContentType);

        StorageItemPathGenerator pathGenerator = new StorageItemPathGenerator() { };
        for (Class<? extends StorageItemPathGenerator> generatorClass : ClassFinder.findConcreteClasses(StorageItemPathGenerator.class)) {
            StorageItemPathGenerator candidate = TypeDefinition.getInstance(generatorClass).newInstance();
            if (candidate.isSupported(storageName)) {
                pathGenerator = candidate;
            }
        }

        storageItem.setPath(pathGenerator.createPath(fileName));

        return storageItem;
    }

    private static StorageItem createStorageItem(String jsonString) {
        Preconditions.checkNotNull(jsonString);
        Map<String, Object> json = Preconditions.checkNotNull(
                ObjectUtils.to(new TypeReference<Map<String, Object>>() { }, ObjectUtils.fromJson(jsonString)));
        Object path = Preconditions.checkNotNull(json.get("path"));
        String storage = ObjectUtils
                .firstNonBlank(json.get("storage"), Settings.get(StorageItem.DEFAULT_STORAGE_SETTING))
                .toString();
        String contentType = ObjectUtils.to(String.class, json.get("contentType"));

        Map<String, Object> metadata = null;
        if (!ObjectUtils.isBlank(json.get("metadata"))) {
            metadata = ObjectUtils.to(
                    new TypeReference<Map<String, Object>>() { },
                    json.get("metadata"));
        }

        StorageItem storageItem = StorageItem.Static.createIn(storage);
        storageItem.setContentType(contentType);
        storageItem.setPath(path.toString());
        storageItem.setMetadata(metadata);

        return storageItem;
    }
}
