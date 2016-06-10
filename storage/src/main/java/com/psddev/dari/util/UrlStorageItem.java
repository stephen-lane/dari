package com.psddev.dari.util;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * Storage item backed by a URL.
 */
public class UrlStorageItem extends AbstractStorageItem {

    /**
     * Storage name assigned to all instances by default.
     */
    public static final String DEFAULT_STORAGE = "_url";

    /**
     * Creates an instance.
     */
    public UrlStorageItem() {
        super.setStorage(DEFAULT_STORAGE);
    }

    @Override
    public void setStorage(String storage) {
    }

    @Override
    public void setPath(String path) {
        super.setPath(path);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpUriRequest request = RequestBuilder.head()
                    .setUri(getPublicUrl())
                    .build();

            try (CloseableHttpResponse response = client.execute(request)) {
                EntityUtils.consume(response.getEntity());

                Header contentTypeHeader = response.getFirstHeader("Content-Type");

                if (contentTypeHeader != null) {
                    super.setContentType(contentTypeHeader.getValue());
                }
            }

        } catch (IOException error) {
            // Ignore.
        }
    }

    @Override
    public void setContentType(String contentType) {
    }

    @Override
    protected InputStream createData() throws IOException {
        URLConnection connection = new URL(getPublicUrl()).openConnection();

        connection.setConnectTimeout(1000);
        connection.setReadTimeout(5000);

        return connection.getInputStream();
    }

    @Override
    protected void saveData(InputStream data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInStorage() {
        return true;
    }
}
