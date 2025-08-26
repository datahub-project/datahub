package io.acryl.admin.grafana;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GrafanaServlet extends ProxyServlet {
  @Autowired
  @Qualifier("grafanaConfig")
  private GrafanaConfiguration.Config grafanaConfig;

  @Autowired
  @Qualifier("grafanaRequiredParameters")
  private List<Map.Entry<String, String[]>> requiredParameters;

  @Autowired
  @Qualifier("grafanaDashboards")
  private Map<String, String> grafanaDashboards;

  @Autowired
  @Qualifier("grafanaAllow")
  private Set<String> grafanaAllow;

  @Autowired
  @Qualifier("grafanaDeny")
  private Set<String> grafanaDeny;

  @Autowired
  @Qualifier("grafanaRedirectDefault")
  private Set<String> grafanaRedirectDefault;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);
    log.info("Initialized {}", getClass().getSimpleName());
  }

  @Override
  protected String getConfigParam(String key) {
    switch (key) {
      case P_TARGET_URI:
        return grafanaConfig.getGrafanaUri().toString();
      case P_PRESERVEHOST:
        return "false";
      case P_LOG:
        return grafanaConfig.getLogging();
      default:
        return super.getConfigParam(key);
    }
  }

  private boolean hasRequiredParameters(HttpServletRequest servletRequest) {
    Map<String, String[]> requestParams = servletRequest.getParameterMap();
    boolean result =
        requiredParameters.stream()
            .allMatch(
                expected ->
                    requestParams.containsKey(expected.getKey())
                        && Arrays.equals(
                            requestParams.get(expected.getKey()), expected.getValue()));

    if (!result && doLog) {
      for (Map.Entry<String, String[]> expected : requiredParameters) {
        if (!requestParams.containsKey(expected.getKey())) {
          log("Missing key: " + expected.getKey());
        } else if (!Arrays.equals(requestParams.get(expected.getKey()), expected.getValue())) {
          log(
              "Key: `"
                  + expected.getKey()
                  + "` value mismatch: `"
                  + String.join(",", expected.getValue())
                  + "` != `"
                  + String.join(",", requestParams.get(expected.getKey()))
                  + "`");
        }
      }
    }

    return result;
  }

  private Stream<String> reduceParams(Stream<Map.Entry<String, String[]>> parameters) {
    return parameters.flatMap(
        paramEntry ->
            paramEntry.getValue().length < 1
                ? Stream.of(paramEntry.getKey())
                : Arrays.stream(paramEntry.getValue())
                    .map(
                        val ->
                            !val.isEmpty()
                                ? String.format("%s=%s", paramEntry.getKey(), val)
                                : paramEntry.getKey()));
  }

  /**
   * Force specific query parameters to always be present with specific value (configuration
   * defined). Add the original parameters as long as they are not the forced parameters. Add any
   * extra parameters, again as long as they are not the forced parameters.
   *
   * @param servletRequest original request
   * @param extraQueryParams extra parameters to add to incoming parameters
   * @return the query string
   */
  private String buildForcedQueryParams(
      HttpServletRequest servletRequest, String extraQueryParams) {
    final Set<String> restricted =
        requiredParameters.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    Stream<String> originParams =
        reduceParams(
            servletRequest.getParameterMap().entrySet().stream()
                .filter(paramEntry -> !restricted.contains(paramEntry.getKey())));
    Stream<String> extraParams =
        Arrays.stream(Optional.ofNullable(extraQueryParams).orElse("").split("&"))
            .filter(paramEntry -> !restricted.contains(paramEntry.split("&", 2)[0]));
    Stream<String> requiredParams = reduceParams(requiredParameters.stream());

    return Stream.concat(
            Stream.concat(originParams, extraParams).filter(p -> !p.isEmpty()), requiredParams)
        .collect(Collectors.joining("&"));
  }

  private String buildRedirect(
      HttpServletRequest servletRequest, String path, String queryParameters) {
    String proto =
        servletRequest.getHeader("X-Forwarded-Proto") != null
            ? servletRequest.getHeader("X-Forwarded-Proto")
            : servletRequest.getScheme();
    String host =
        servletRequest.getHeader("X-Forwarded-Host") != null
            ? servletRequest.getHeader("X-Forwarded-Host")
            : servletRequest.getRemoteHost();
    String forward = proto + "://" + host;

    StringBuilder uri = new StringBuilder(500);
    uri.append(forward);
    uri.append(servletRequest.getServletPath());
    if (doLog) {
      log("redirect: " + uri);
    }
    uri.append(Optional.ofNullable(path).orElse(""));
    if (doLog) {
      log("redirect: " + uri);
    }

    // Handle the query string
    uri.append('?');
    // queryString is not decoded, so we need encodeUriQuery not to encode "%" characters, to avoid
    // double-encoding
    uri.append(encodeUriQuery(queryParameters, false));
    if (doLog) {
      log("redirect: " + uri);
    }

    return uri.toString();
  }

  @Override
  protected void service(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
      throws ServletException, IOException {

    if (grafanaDeny.stream().anyMatch(deny -> servletRequest.getPathInfo().startsWith(deny))) {
      if (doLog) {
        log("proxy denied by rule: " + servletRequest.getRequestURI());
      }
      servletResponse.setStatus(403);
      return;
    }

    if (grafanaRedirectDefault.stream()
        .anyMatch(redirect -> servletRequest.getPathInfo().startsWith(redirect))) {
      if (doLog) {
        log(
            "redirect default ("
                + grafanaConfig.getDefaultDashboard()
                + "): "
                + servletRequest.getRequestURI());
      }
      redirectVanity(servletRequest, servletResponse, grafanaConfig.getDefaultDashboard());
      return;
    }

    if (servletRequest.getMethod().equals("GET")
        && servletRequest.getPathInfo().startsWith("/d/")
        && !hasRequiredParameters(servletRequest)) {
      /*
       * Enforce required parameters by redirect
       */
      if (doLog) {
        log("Enforcing parameters");
      }
      servletResponse.sendRedirect(
          buildRedirect(
              servletRequest,
              servletRequest.getPathInfo(),
              buildForcedQueryParams(servletRequest, null)));
    } else if (servletRequest.getMethod().equals("GET")
        && grafanaDashboards.keySet().stream()
            .anyMatch(vanity -> servletRequest.getPathInfo().equals(vanity))) {
      /*
       * Handle vanity endpoints
       */
      if (doLog) {
        log("Handling vanity endpoint");
      }
      String dest =
          grafanaDashboards.entrySet().stream()
              .filter(vanity -> servletRequest.getPathInfo().equals(vanity.getKey()))
              .map(Map.Entry::getValue)
              .findFirst()
              .get();

      redirectVanity(servletRequest, servletResponse, dest);
    } else if (servletRequest.getPathInfo() == null
        || grafanaAllow.stream().anyMatch(servletRequest.getPathInfo()::startsWith)) {
      super.service(servletRequest, servletResponse);
    } else if (servletRequest.getPathInfo().startsWith("/d/")) {
      if (doLog) {
        log("proxy denied non-allowed dashboard: " + servletRequest.getRequestURI());
      }
      servletResponse.setStatus(403);
    } else {
      if (doLog) {
        log("proxy denied silent: " + servletRequest.getRequestURI());
      }
    }
  }

  private void redirectVanity(
      HttpServletRequest servletRequest, HttpServletResponse servletResponse, String dest)
      throws IOException {
    String[] splitDest = dest.split("[?]", 2);
    String destPath = splitDest[0];
    String destQuery = dest.contains("?") ? splitDest[1] : null;

    servletResponse.sendRedirect(
        buildRedirect(servletRequest, destPath, buildForcedQueryParams(servletRequest, destQuery)));
  }

  @Override
  protected void copyRequestHeaders(
      HttpServletRequest servletRequest, ClassicHttpRequest proxyRequest) {
    super.copyRequestHeaders(servletRequest, proxyRequest);
    proxyRequest.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + grafanaConfig.getGrafanaToken());
    proxyRequest.setHeader(HttpHeaders.HOST, grafanaConfig.getGrafanaUri().getHost());
  }

  @Override
  protected void copyResponseEntity(
      ClassicHttpResponse proxyResponse,
      HttpServletResponse servletResponse,
      ClassicHttpRequest proxyRequest,
      HttpServletRequest servletRequest)
      throws IOException {
    HttpEntity entity = proxyResponse.getEntity();
    if (entity != null) {
      OutputStream servletOutputStream = servletResponse.getOutputStream();
      if (isRewritable(proxyResponse, "html")) {
        try {
          String body =
              modifyResponseBody(
                  servletRequest,
                  new String(entity.getContent().readAllBytes(), StandardCharsets.UTF_8),
                  "html");
          servletOutputStream.write(body.getBytes(StandardCharsets.UTF_8));
        } finally {
          servletOutputStream.flush();
        }
      } else {
        // parent's default behavior
        entity.writeTo(servletOutputStream);
      }
    }
  }

  protected String modifyResponseBody(HttpServletRequest servletRequest, String body, String type) {
    switch (type) {
      case "html":
        // "appSubUrl":"/admin/dashboard","appUrl":"http://localhost:3000/admin/dashboard/"
        body =
            body.replaceFirst(
                "\"appUrl\":\".*?\"",
                "\"appUrl\":\""
                    + servletRequest.getScheme()
                    + servletRequest.getHeader(HttpHeaders.HOST)
                    + servletRequest.getServletPath()
                    + "\"");
        body =
            body.replaceFirst(
                "\"appSubUrl\":\".*?\"",
                "\"appSubUrl\":\"" + servletRequest.getServletPath() + "\"");
        body =
            body.replaceFirst(
                "<base href=\".*?\"/>",
                "<base href=\"" + servletRequest.getServletPath() + "/\"/>");
        break;
      default:
        break;
    }

    return body;
  }

  private boolean isRewritable(HttpResponse httpResponse, String type) {
    boolean rewriteable = false;
    Header[] contentTypeHeaders = httpResponse.getHeaders("Content-Type");
    for (Header header : contentTypeHeaders) {
      // May need to accept other types
      if (header.getValue().contains(type)) {
        rewriteable = true;
      }
    }
    return rewriteable;
  }
}
