package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Automatically enables {@link ReusableRequest}.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class ReusableRequestFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        chain.doFilter(new ReusableRequest(request), response);
    }
}
