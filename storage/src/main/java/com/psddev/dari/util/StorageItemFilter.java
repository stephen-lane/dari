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

    private static final String DEFAULT_UPLOAD_PATH = "/_dari/upload";
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

        if (request.getServletPath().equals(Settings.getOrDefault(String.class, StorageItem.SETTING_PREFIX + "/uploadPath", DEFAULT_UPLOAD_PATH))) {
            WebPageContext page = new WebPageContext((ServletContext) null, request, response);

            String fileParam = page.param(String.class, FILE_PARAM);
            String storageName = page.param(String.class, STORAGE_PARAM);

            Object responseObject = StorageItemFilter.getParameter(request, fileParam, storageName);

            response.setContentType("application/json");
            page.write(ObjectUtils.toJson(responseObject));
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

        StorageItem storageItem = null;

        MultipartRequest mpRequest = MultipartRequestFilter.Static.getInstance(request);

        if (mpRequest != null) {
            FileItem item = mpRequest.getFileItem(paramName);

            if (item != null) {
                if (item.isFormField()) {
                    storageItem = createStorageItem(mpRequest.getParameter(paramName));
                } else {
                    StorageItemPart part = new StorageItemPart();
                    part.setFileItem(item);
                    part.setStorageName(storageName);

                    storageItem = createStorageItem(part);
                }
            }
        } else {
            storageItem = createStorageItem(request.getParameter(paramName));
        }

        return storageItem;
    }

    private static StorageItem createStorageItem(String jsonString) {
        Preconditions.checkNotNull(jsonString);
        Map<String, Object> json = Preconditions.checkNotNull(
                ObjectUtils.to(
                        new TypeReference<Map<String, Object>>() { },
                        ObjectUtils.fromJson(jsonString)));
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

    private static StorageItem createStorageItem(StorageItemPart part) throws IOException {

        String storageName = StringUtils.isBlank(part.getStorageName()) ? Settings.get(String.class, StorageItem.DEFAULT_STORAGE_SETTING) : part.getStorageName();

        File file;
        try {
            file = File.createTempFile("cms.", ".tmp");
            part.getFileItem().write(file);
        } catch (Exception e) {
            throw new IOException("Unable to write [" + (StringUtils.isBlank(part.getName()) ? part.getName() : "fileItem") + "] to temporary file.", e);
        }

        part.setFile(file);

        // Add additional validation by creating StorageItemValidators
        validateStorageItem(part);

        StorageItem storageItem = StorageItem.Static.createIn(storageName);
        storageItem.setContentType(part.getContentType());
        storageItem.setPath(getPathGenerator(storageName).createPath(part.getName()));
        storageItem.setData(new FileInputStream(file));

        part.setStorageItem(storageItem);

        // Add additional preprocessing by creating StorageItemPreprocessors
        preprocessStorageItem(part);

        // Add postprocessing by creating StorageItemPostprocessors
        addPostprocessingListener(part);

        storageItem.save();

        return storageItem;
    }

    private static void validateStorageItem(final StorageItemPart part) {

        FileItem fileItem = part.getFileItem();
        String fileName = Preconditions.checkNotNull(fileItem.getName());

        Preconditions.checkState(fileItem.getSize() > 0,
                "File [" + fileName + "] is empty");

        ClassFinder.findConcreteClasses(StorageItemValidator.class)
                .forEach(c -> {
                    try {
                        StorageItemValidator validator = TypeDefinition.getInstance(c).newInstance();
                        if (validator.isSupported(part.getStorageName())) {
                            validator.validate(part);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private static void preprocessStorageItem(final StorageItemPart part) {
        ClassFinder.findConcreteClasses(StorageItemPreprocessor.class)
                .forEach(c -> TypeDefinition.getInstance(c).newInstance().process(part));
    }

    private static void addPostprocessingListener(StorageItemPart part) {
        StorageItem storageItem = part.getStorageItem();
        if (storageItem instanceof AbstractStorageItem) {
            AbstractStorageItem abstractStorageItem = (AbstractStorageItem) storageItem;
            abstractStorageItem.registerListener((listener) -> {
                // TODO: offload this to background task here or in StorageItemListener?
                // TODO: handle execution ordering?
                ClassFinder.findConcreteClasses(StorageItemPostprocessor.class)
                        .forEach(c -> TypeDefinition.getInstance(c).newInstance().process(part));
            });
        }
    }

    private static StorageItemPathGenerator getPathGenerator(final String storageName) {

        StorageItemPathGenerator pathGenerator = new StorageItemPathGenerator() { };
        for (Class<? extends StorageItemPathGenerator> generatorClass : ClassFinder.findConcreteClasses(StorageItemPathGenerator.class)) {

            if (generatorClass.getCanonicalName() == null) {
                continue;
            }

            StorageItemPathGenerator candidate = TypeDefinition.getInstance(generatorClass).newInstance();
            double candidatePriority = candidate.getPriority(storageName);
            double highestPriority = pathGenerator.getPriority(storageName);

            Preconditions.checkState(candidatePriority != highestPriority,
                    "Priorities of [" + candidate.getClass().getSimpleName() + "] and [" + pathGenerator.getClass().getSimpleName() + "] are ambiguous. Priorities should not be the same.");

            if (candidatePriority  > highestPriority) {
                pathGenerator = candidate;
            }
        }

        return pathGenerator;
    }
}
