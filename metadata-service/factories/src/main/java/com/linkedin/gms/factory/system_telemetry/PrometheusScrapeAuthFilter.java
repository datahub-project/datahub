package com.linkedin.gms.factory.system_telemetry;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Optional HTTP Basic authentication for {@code /actuator/prometheus}.
 *
 * <p>Independent of DataHub token auth. When disabled, requests pass through unchanged.
 */
@Slf4j
public class PrometheusScrapeAuthFilter extends OncePerRequestFilter {

  public static final String PROMETHEUS_ACTUATOR_PATH = "/actuator/prometheus";
  private static final String BASIC_PREFIX = "Basic ";

  private final PrometheusScrapeAuthSettings settings;

  public PrometheusScrapeAuthFilter(PrometheusScrapeAuthSettings settings) {
    this.settings = settings;
  }

  @PostConstruct
  public void validateConfiguration() {
    if (!settings.isEnabled()) {
      return;
    }
    log.info("Prometheus scrape HTTP Basic auth enabled for {}", PROMETHEUS_ACTUATOR_PATH);
    if (settings.isUsingDefaultPassword()) {
      log.warn(
          "Prometheus scrape auth is using the default password ({}). Set "
              + "MANAGEMENT_METRICS_EXPORT_PROMETHEUS_AUTH_PASSWORD to a strong secret.",
          PrometheusScrapeAuthSettings.DEFAULT_PASSWORD);
    }
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    if (!settings.isEnabled()) {
      return true;
    }
    String path = request.getServletPath();
    return path == null || !PROMETHEUS_ACTUATOR_PATH.equals(path);
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {

    if (isAuthorized(request)) {
      filterChain.doFilter(request, response);
      return;
    }

    response.setHeader("WWW-Authenticate", "Basic realm=\"Prometheus\"");
    response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
  }

  private boolean isAuthorized(HttpServletRequest request) {
    String authorization = request.getHeader("Authorization");
    if (!StringUtils.hasText(authorization) || !authorization.startsWith(BASIC_PREFIX)) {
      return false;
    }

    try {
      byte[] decoded =
          Base64.getDecoder().decode(authorization.substring(BASIC_PREFIX.length()).trim());
      String credentials = new String(decoded, StandardCharsets.UTF_8);
      int separator = credentials.indexOf(':');
      if (separator < 0) {
        return false;
      }
      String username = credentials.substring(0, separator);
      String password = credentials.substring(separator + 1);
      return constantTimeEquals(username, settings.getUsername())
          && constantTimeEquals(password, settings.getPassword());
    } catch (IllegalArgumentException e) {
      log.debug("Invalid Basic authorization header on {}", PROMETHEUS_ACTUATOR_PATH);
      return false;
    }
  }

  private static boolean constantTimeEquals(String actual, String expected) {
    if (actual == null || expected == null) {
      return false;
    }
    return MessageDigest.isEqual(
        actual.getBytes(StandardCharsets.UTF_8), expected.getBytes(StandardCharsets.UTF_8));
  }
}
