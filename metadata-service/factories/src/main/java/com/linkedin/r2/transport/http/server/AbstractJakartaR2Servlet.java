package com.linkedin.r2.transport.http.server;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.*;
import com.linkedin.r2.transport.common.WireAttributeHelper;
import com.linkedin.r2.transport.common.bridge.common.TransportResponse;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;
import com.linkedin.r2.transport.http.common.HttpConstants;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractJakartaR2Servlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final Duration timeout;

  protected abstract HttpDispatcher getDispatcher();

  protected AbstractJakartaR2Servlet(Duration timeout) {
    this.timeout = timeout;
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      RequestContext requestContext = JakartaServletHelper.readRequestContext(req);
      RestRequest restRequest = createRestRequest(req);

      CompletableFuture<TransportResponse<RestResponse>> future = new CompletableFuture<>();

      getDispatcher().handleRequest(restRequest, requestContext, future::complete);

      TransportResponse<RestResponse> result =
          future
              .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
              .exceptionally(this::handleException)
              .join();

      writeResponse(result, resp);

    } catch (URISyntaxException e) {
      writeError(resp, RestStatus.BAD_REQUEST, "Invalid URI: " + e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error processing request", e);
      writeError(resp, RestStatus.INTERNAL_SERVER_ERROR, "Internal server error");
    }
  }

  private TransportResponse<RestResponse> handleException(Throwable ex) {
    if (ex instanceof TimeoutException) {
      RestResponse errorResponse =
          RestStatus.responseForError(
              RestStatus.INTERNAL_SERVER_ERROR,
              new RuntimeException("Server timeout after " + timeout.toSeconds() + " seconds"));
      return TransportResponseImpl.error(new RestException(errorResponse));
    }
    return TransportResponseImpl.error(ex);
  }

  private RestRequest createRestRequest(HttpServletRequest req)
      throws IOException, ServletException, URISyntaxException {
    String pathInfo = extractPathInfo(req);
    String queryString = Optional.ofNullable(req.getQueryString()).map(q -> "?" + q).orElse("");

    URI uri = new URI(pathInfo + queryString);

    RestRequestBuilder builder = new RestRequestBuilder(uri).setMethod(req.getMethod());

    // Handle headers
    Collections.list(req.getHeaderNames())
        .forEach(
            headerName -> {
              if (headerName.equalsIgnoreCase(HttpConstants.REQUEST_COOKIE_HEADER_NAME)) {
                Collections.list(req.getHeaders(headerName)).forEach(builder::addCookie);
              } else {
                Collections.list(req.getHeaders(headerName))
                    .forEach(value -> builder.addHeaderValue(headerName, value));
              }
            });

    // Handle request body
    int contentLength = req.getContentLength();
    ByteString entity =
        (contentLength > 0)
            ? ByteString.read(req.getInputStream(), contentLength)
            : ByteString.read(req.getInputStream());

    builder.setEntity(entity);

    return builder.build();
  }

  private void writeResponse(TransportResponse<RestResponse> response, HttpServletResponse resp)
      throws IOException {
    // Write wire attributes
    WireAttributeHelper.toWireAttributes(response.getWireAttributes()).forEach(resp::setHeader);

    // Get response or create error response
    RestResponse restResponse =
        Optional.of(response)
            .filter(TransportResponse::hasError)
            .map(
                r -> {
                  Throwable error = r.getError();
                  if (error instanceof RestException) {
                    return ((RestException) error).getResponse();
                  }
                  return RestStatus.responseForError(RestStatus.INTERNAL_SERVER_ERROR, error);
                })
            .orElseGet(response::getResponse);

    // Write status and headers
    resp.setStatus(restResponse.getStatus());
    restResponse.getHeaders().forEach(resp::setHeader);

    // Write cookies
    restResponse
        .getCookies()
        .forEach(cookie -> resp.addHeader(HttpConstants.RESPONSE_COOKIE_HEADER_NAME, cookie));

    // Write response body
    try (var outputStream = resp.getOutputStream()) {
      restResponse.getEntity().write(outputStream);
    }
  }

  private void writeError(HttpServletResponse resp, int statusCode, String message)
      throws IOException {
    RestResponse errorResponse = RestStatus.responseForStatus(statusCode, message);
    writeResponse(TransportResponseImpl.success(errorResponse), resp);
  }

  protected static String extractPathInfo(HttpServletRequest req) throws ServletException {
    String requestUri = req.getRequestURI();
    String contextPath = Optional.ofNullable(req.getContextPath()).orElse("");
    String servletPath = Optional.ofNullable(req.getServletPath()).orElse("");

    String prefix = contextPath + servletPath;
    String pathInfo = null;

    if (prefix.isEmpty()) {
      pathInfo = requestUri;
    } else if (servletPath.startsWith("/gms") && requestUri.startsWith(prefix)) {
      pathInfo = requestUri.substring(prefix.length());
    }

    if (pathInfo == null || pathInfo.isEmpty()) {
      log.debug(
          "Previously invalid servlet mapping detected. Request details: method='{}', requestUri='{}', contextPath='{}', "
              + "servletPath='{}', prefix='{}', pathInfo='{}', queryString='{}', protocol='{}', remoteAddr='{}', "
              + "serverName='{}', serverPort={}, contentType='{}', characterEncoding='{}'",
          req.getMethod(),
          requestUri,
          contextPath,
          servletPath,
          prefix,
          pathInfo,
          req.getQueryString(),
          req.getProtocol(),
          req.getRemoteAddr(),
          req.getServerName(),
          req.getServerPort(),
          req.getContentType(),
          req.getCharacterEncoding());

      /*  NOTE: Working around what was previously considered an error.
       *       throw new ServletException(
       *       "R2 servlet must be mapped using wildcard path mapping (e.g., /r2/*). " +
       *               "Exact path matching (/r2) and file extension mappings (*.r2) are not supported.");
       **/

      pathInfo = requestUri;
    }

    return pathInfo;
  }
}
