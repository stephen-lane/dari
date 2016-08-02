package com.psddev.dari.db;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.JspUtils;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.ProfilerFilter;

/**
 * Enables {@link ProfilingDatabase} if {@link Profiler} is active
 * on the current HTTP request.
 */
public class ProfilingDatabaseFilter extends AbstractFilter {

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        Profiler profiler = Profiler.Static.getThreadProfiler();

        if (profiler == null) {
            super.doRequest(request, response, chain);

        } else {
            ProfilingDatabase profiling = new ProfilingDatabase();
            profiling.setDelegate(Database.Static.getDefault());

            HtmlWriter resultWriter = ProfilerFilter.Static.getResultWriter(request, response);

            resultWriter.putOverride(Recordable.class, (writer, recordable) -> {
                if (recordable instanceof Query) {
                    ((Query<?>) recordable).format(writer);
                    return;
                }

                State recordableState = recordable.getState();
                ObjectType type = recordableState.getType();

                if (type != null) {
                    writer.writeHtml(type.getLabel());
                    writer.writeHtml(": ");
                }

                writer.writeStart("a", "href", JspUtils.getAbsolutePath(
                        request, "/_debug/query",
                        "where", "id = " + recordableState.getId(),
                        "event", "Run"), "target", "query");
                writer.writeHtml(recordableState.getLabel());
                writer.writeEnd();
            });

            resultWriter.putOverride(State.class, (writer, state) -> {
                ObjectType type = state.getType();

                if (type != null) {
                    writer.writeHtml(type.getLabel());
                    writer.writeHtml(": ");
                }

                writer.writeStart("a", "href", JspUtils.getAbsolutePath(
                        request, "/_debug/query",
                        "where", "id = " + state.getId(),
                        "event", "Run"), "target", "query");
                writer.writeHtml(state.getLabel());
                writer.writeEnd();
            });

            try {
                Database.Static.overrideDefault(profiling);
                super.doRequest(request, response, chain);

            } finally {
                Database.Static.restoreDefault();
            }
        }
    }
}
