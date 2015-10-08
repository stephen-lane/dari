package com.psddev.dari.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter that exposes {@link LocalStorageItem} on the web.
 */
public class LocalStorageFilter extends AbstractFilter {

    private static final String DARI_STORAGE_SETTING = "dari/storage";
    private static final String ROOT_PATH_SETTING = "rootPath";

    private String localStorageRootPath;

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<>();

        dependencies.add(StandardFilter.class);

        return dependencies;
    }

    @Override
    protected void doInit() {
        Map<String, Object> settings = Settings.asMap();

        for (String key : settings.keySet()) {
            if (key.startsWith(DARI_STORAGE_SETTING) && key.endsWith(ROOT_PATH_SETTING)) {
                localStorageRootPath = (String) settings.get(key);
                break;
            }
        }
    }

    @Override
    protected void doRequest(
               HttpServletRequest request,
               HttpServletResponse response,
               FilterChain chain)
               throws IOException,
               ServletException {

        if (!StringUtils.isBlank(localStorageRootPath)) {
            // parse request path and see if on disk, return or carry on
            String requestUri = request.getRequestURI();

            File f = new File(localStorageRootPath + requestUri);
            if (f.exists() && !f.isDirectory()) {
                ServletOutputStream out = response.getOutputStream();
                String mimeType = ObjectUtils.getContentType(f.getName());

                response.setHeader("Content-Type", mimeType);
                response.setContentLength((int) f.length());

                BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(f));

                int b;
                while ((b = inputStream.read()) != -1) {
                    out.write(b);
                }

                inputStream.close();
                out.close();

                return;
            }
        }

        chain.doFilter(request, response);
    }
}
