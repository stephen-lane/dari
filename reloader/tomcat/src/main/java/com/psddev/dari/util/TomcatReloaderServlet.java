package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import org.apache.catalina.ContainerServlet;
import org.apache.catalina.Context;
import org.apache.catalina.Host;
import org.apache.catalina.Wrapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Reloads Tomcat web application for {@link SourceFilter}.
 */
public class TomcatReloaderServlet extends HttpServlet implements ContainerServlet {

    private static final long serialVersionUID = 1L;

    private static final String WAIT_ACTION = "wait";

    private Wrapper wrapper;
    private Host host;
    private final AtomicBoolean reloading = new AtomicBoolean();

    @Override
    public Wrapper getWrapper() {
        return this.wrapper;
    }

    @Override
    public void setWrapper(Wrapper wrapper) {
        if (wrapper == null) {
            this.wrapper = null;
            this.host = null;

        } else {
            this.wrapper = wrapper;
            this.host = (Host) wrapper.getParent().getParent();
        }
    }

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        String action = request.getParameter(SourceFilter.RELOADER_ACTION_PARAMETER);

        // Ping to let SourceFilter know that reloader is installed.
        if (SourceFilter.RELOADER_PING_ACTION.equals(action)) {
            response.setContentType("text/plain");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write("OK");
            return;
        }

        String contextPath = Preconditions.checkNotNull(
                request.getParameter(SourceFilter.RELOADER_CONTEXT_PATH_PARAMETER),
                "[%s] parameter is required!",
                SourceFilter.RELOADER_CONTEXT_PATH_PARAMETER);

        String requestPath = Preconditions.checkNotNull(
                request.getParameter(SourceFilter.RELOADER_REQUEST_PATH_PARAMETER),
                "[%s] parameter is required!",
                SourceFilter.RELOADER_REQUEST_PATH_PARAMETER);

        // Reload requested.
        if (SourceFilter.RELOADER_RELOAD_ACTION.equals(action)) {

            // Display the reloading message immediately.
            writeMessage(request, response, contextPath, requestPath);

            // In case of concurrent requests to reload.
            if (reloading.compareAndSet(false, true)) {
                boolean reloaderStarted = false;

                try {
                    Context context = (Context) host.findChild(contextPath);

                    if (context != null) {
                        Thread reloader = new Thread(() -> {
                            try {

                                // The wait is necessary to make sure that the
                                // reloading message has a chance to display,
                                // since Tomcat seems to block everything during
                                // reload.
                                try {
                                    Thread.sleep(2000);

                                } catch (InterruptedException error) {
                                    // Safe to ignore.
                                }

                                context.reload();

                            } finally {
                                reloading.compareAndSet(true, false);
                            }
                        });

                        reloader.setDaemon(true);
                        reloader.start();
                        reloaderStarted = true;

                    } else {
                        throw new IllegalArgumentException(String.format(
                                "No context matching [%s]!", contextPath));
                    }

                } finally {
                    if (!reloaderStarted) {
                        reloading.compareAndSet(true, false);
                    }
                }
            }

            return;
        }

        // Wait for reload to finish.
        if (WAIT_ACTION.equals(action)) {
            boolean ajax = JspUtils.isAjaxRequest(request);
            boolean finished = false;

            try (CloseableHttpClient pingClient = HttpClients.createDefault()) {
                try (CloseableHttpResponse pingResponse = pingClient.execute(RequestBuilder.get()
                        .setUri(JspUtils.getHostUrl(request) + contextPath + SourceFilter.Static.getInterceptPingPath())
                        .setConfig(RequestConfig.custom()
                                .setConnectionRequestTimeout(500)
                                .setConnectTimeout(500)
                                .setSocketTimeout(500)
                                .build())
                        .build())) {

                    if ("OK".equals(EntityUtils.toString(pingResponse.getEntity()))) {
                        finished = true;
                    }
                }

            } catch (IOException error) {
                // Reload isn't finished yet.
            }

            if (ajax) {
                response.setContentType("text/plain");
                response.setCharacterEncoding("UTF-8");
                response.getWriter().write(String.valueOf(finished));

            } else {
                if (finished) {
                    response.sendRedirect(contextPath + requestPath);

                } else {
                    writeMessage(request, response, contextPath, requestPath);
                }
            }

            return;
        }

        throw new IllegalArgumentException(String.format(
                "[%s] isn't a valid action!",
                action));
    }

    private void writeMessage(
            HttpServletRequest request,
            HttpServletResponse response,
            String contextPath,
            String requestPath)
            throws IOException {

        response.setContentType("text/html");
        response.setCharacterEncoding("UTF-8");

        HtmlWriter writer = new HtmlWriter(response.getWriter());
        String waitUrl = new UrlBuilder(request)
                .currentPath()
                .currentParameters()
                .parameter(SourceFilter.RELOADER_ACTION_PARAMETER, WAIT_ACTION)
                .toString();

        writer.writeTag("!doctype html");
        writer.writeStart("html");
        {
            writer.writeStart("head");
            {
                writer.writeStart("title").writeHtml("Reloader").writeEnd();

                writer.writeStart("style", "type", "text/css");
                writeResource(writer, "TomcatReloaderServlet.css");
                writer.writeEnd();

                writer.writeStart("noscript");
                {
                    writer.writeElement("meta",
                            "http-equiv", "refresh",
                            "content", "2; url=" + waitUrl);
                }
                writer.writeEnd();

                writer.writeStart("script", "type", "text/javascript");
                writer.writeRaw("var RELOADER_WAIT_URL = '").writeRaw(StringUtils.escapeJavaScript(waitUrl)).writeRaw("';");
                writer.writeRaw("var RELOADER_RETURN_URL = '").writeRaw(StringUtils.escapeJavaScript(contextPath + requestPath)).writeRaw("';");
                writer.writeEnd();

                writer.writeStart("script", "type", "text/javascript");
                writeResource(writer, "TomcatReloaderServlet.js");
                writer.writeEnd();
            }
            writer.writeEnd();

            writer.writeStart("body");
            {
                writer.writeStart("div", "class", "container");
                {
                    writer.writeStart("div", "class", "message");
                    {
                        writer.writeHtml("rel");
                        writer.writeStart("span", "class", "o");
                        writer.writeEnd();
                        writer.writeHtml("ading...");
                    }
                    writer.writeEnd();
                }
                writer.writeEnd();
            }
            writer.writeEnd();
        }
        writer.writeEnd();
    }

    private void writeResource(HtmlWriter writer, String name) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(
                getClass().getResourceAsStream(name),
                StandardCharsets.UTF_8)) {

            CharStreams.copy(reader, writer);
        }
    }
}
