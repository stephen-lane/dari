package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import javax.servlet.ServletContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * {@link CdnContext} implementation that uses {@link ServletContext} APIs.
 */
public class ServletCdnContext implements CdnContext {

    private final ServletContext servletContext;

    /**
     * Creates an instance based on the given {@code servletContext}.
     *
     * @param servletContext Nonnull.
     */
    public ServletCdnContext(ServletContext servletContext) {
        Preconditions.checkNotNull(servletContext);

        this.servletContext = servletContext;
    }

    @Override
    public long getLastModified(String servletPath) throws IOException {
        Preconditions.checkNotNull(servletPath);

        String realPath = servletContext.getRealPath(servletPath);

        if (realPath == null) {
            URL resource = servletContext.getResource(servletPath);

            if (resource == null) {
                throw new FileNotFoundException(servletPath);
            }

            URLConnection resourceConnection = resource.openConnection();

            try {
                return resourceConnection.getLastModified();

            } finally {
                resourceConnection.getInputStream().close();
            }
        }

        return Files.getLastModifiedTime(Paths.get(realPath)).toMillis();
    }

    @Override
    public InputStream open(String servletPath) throws IOException {
        InputStream input = servletContext.getResourceAsStream(servletPath);

        if (input == null) {
            throw new FileNotFoundException(servletPath);
        }

        return input;
    }

    @Override
    public String getPathPrefix() {
        return servletContext.getContextPath();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || (other instanceof ServletCdnContext
                && servletContext.equals(((ServletCdnContext) other).servletContext));
    }
    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(servletContext);
    }
}

