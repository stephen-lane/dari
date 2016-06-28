package com.psddev.dari.util;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import org.apache.catalina.ContainerServlet;
import org.apache.catalina.Context;
import org.apache.catalina.Host;
import org.apache.catalina.Wrapper;

/** Reloads a web application in Tomcat for {@link SourceFilter}. */
public class TomcatReloaderServlet extends HttpServlet implements ContainerServlet {

    private static final long serialVersionUID = 1L;

    private Wrapper wrapper;
    private Host host;
    private final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>();

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

        if (SourceFilter.RELOADER_PING_ACTION.equals(action)) {
            response.setContentType("text/plain");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write("OK");
            return;
        }

        Preconditions.checkArgument(
                SourceFilter.RELOADER_RELOAD_ACTION.equals(action),
                "[%s] isn't a valid reloader action!",
                action);

        String contextPath = Preconditions.checkNotNull(
                request.getParameter(SourceFilter.RELOADER_CONTEXT_PATH_PARAMETER),
                "[%s] parameter is required!",
                SourceFilter.RELOADER_CONTEXT_PATH_PARAMETER);

        String requestPath = Preconditions.checkNotNull(
                request.getParameter(SourceFilter.RELOADER_REQUEST_PATH_PARAMETER),
                "[%s] parameter is required!",
                SourceFilter.RELOADER_REQUEST_PATH_PARAMETER);

        // If context is still loading, display a message and wait.
        response.setContentType("text/html");
        response.setCharacterEncoding("UTF-8");

        new HtmlWriter(response.getWriter()) { {
            putDefault(StackTraceElement.class, HtmlFormatter.STACK_TRACE_ELEMENT);
            putDefault(Throwable.class, HtmlFormatter.THROWABLE);

            writeTag("!doctype html");
            writeStart("html");
            {
                writeStart("head");
                {

                    writeStart("title").writeHtml("Reloader").writeEnd();

                    writeElement("link",
                            "href", JspUtils.getAbsolutePath(request, "/_resource/bootstrap/css/bootstrap.css"),
                            "rel", "stylesheet",
                            "type", "text/css");

                    writeStart("style", "type", "text/css");
                    {
                        write(".hero-unit { background: transparent; left: 0; margin: -72px 0 0 60px; padding: 0; position: absolute; top: 50%; }");
                        write(".hero-unit h1 { line-height: 1.33; }");
                    }
                    writeEnd();
                }
                writeEnd();

                writeStart("body");
                {

                    writeStart("div", "class", "hero-unit");
                    {
                        writeStart("h1");
                        {
                            writeHtml("Reloading ");
                            writeHtml(contextPath);
                            writeHtml("/");
                        }
                        writeEnd();

                        try {
                            writeStart("ul", "class", "muted");

                            try {
                                writeStart("li");
                                writeHtml("Waiting for it to start back up");
                                writeEnd();

                                for (int i = 0; i < 4000; ++i) {
                                    writeHtml(" ");
                                }
                                writeHtml("\r\n");
                                flush();

                                reload(request, response, contextPath, requestPath);

                                writeStart("li");
                                writeHtml("Ready!");
                                writeEnd();

                                writeStart("script", "type", "text/javascript");
                                write("location.href = '" + StringUtils.escapeJavaScript(requestPath) + "';");
                                writeEnd();

                            } finally {
                                writeEnd();
                            }

                        } catch (Exception ex) {
                            writeObject(ex);
                        }
                    }
                    writeEnd();
                }
                writeEnd();
            }
            writeEnd();
        } };
    }

    private void reload(
            HttpServletRequest request,
            HttpServletResponse response,
            String contextPath,
            String requestPath)
            throws IOException {

        CountDownLatch latch = new CountDownLatch(1);

        if (latchReference.compareAndSet(null, latch)) {
            try {
                Context context = (Context) host.findChild(contextPath);

                if (context != null) {
                    context.reload();

                } else {
                    throw new IllegalArgumentException(String.format(
                            "No context matching [%s]!", contextPath));
                }

            } finally {
                latch.countDown();
                latchReference.set(null);
            }

        } else {
            latch = latchReference.get();

            if (latch != null) {
                try {
                    latch.await();

                } catch (InterruptedException ex) {
                    // Ignore thread interruption and continue.
                }
            }
        }

        List<Throwable> errors = new ArrayList<>();

        for (int i = 0; i < 20; ++ i) {
            try {
                URL pingUrl = new URL(JspUtils.getHostUrl(request) + contextPath + SourceFilter.Static.getInterceptPingPath());

                if ("OK".equals(IoUtils.toString(pingUrl))) {
                    return;
                }

            } catch (IOException ex) {
                errors.add(ex);
            }

            try {
                Thread.sleep(2000);

            } catch (InterruptedException ex) {
                break;
            }
        }

        throw new AggregateException(errors);
    }
}
