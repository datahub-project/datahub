package com.linkedin.metadata.config.search;

import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Parsed form of a {@code elasticsearch.clusters.<name>.uri}. The URI is the single source of truth
 * for scheme, host, port and RestClient path prefix.
 *
 * <p>Credentials, IAM, TLS material and thread counts are deliberately <i>not</i> expressed in the
 * URI — they stay as sibling fields on {@link SearchClusterSettings} so secrets never end up in a
 * connection string that gets logged.
 */
@Value
public class SearchClusterUri {
  String scheme;
  boolean useSSL;
  String host;
  int port;

  /**
   * Reverse-proxy path prepended to every request, normalized to a leading slash with no trailing
   * slash. Empty string means "no prefix", matching a blank {@code ELASTICSEARCH_PATH_PREFIX}.
   */
  String pathPrefix;

  /**
   * Normalized {@code scheme://host:port[/path]} used both for RestClient and for alias identity.
   */
  public String normalized() {
    return scheme + "://" + host + ":" + port + pathPrefix;
  }

  /**
   * Parses a cluster URI.
   *
   * @param clusterName cluster key, used only to make failures identifiable
   * @throws IllegalArgumentException if the URI is unusable as a search endpoint
   */
  @Nonnull
  public static SearchClusterUri parse(@Nonnull String clusterName, @Nonnull String uri) {
    final URI parsed;
    try {
      parsed = new URI(uri.trim());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "elasticsearch.clusters." + clusterName + ".uri is not a valid URI: " + uri, e);
    }

    String scheme = parsed.getScheme() == null ? null : parsed.getScheme().toLowerCase();
    if (!"http".equals(scheme) && !"https".equals(scheme)) {
      throw new IllegalArgumentException(
          "elasticsearch.clusters."
              + clusterName
              + ".uri must use http or https, got: "
              + parsed.getScheme());
    }
    if (parsed.getHost() == null || parsed.getHost().isEmpty()) {
      throw new IllegalArgumentException(
          "elasticsearch.clusters." + clusterName + ".uri is missing a host: " + uri);
    }
    // Credentials belong in ELASTICSEARCH_USERNAME/PASSWORD; query and fragment have no meaning
    // for a RestClient path prefix and would silently be dropped.
    if (parsed.getUserInfo() != null) {
      throw new IllegalArgumentException(
          "elasticsearch.clusters."
              + clusterName
              + ".uri must not embed credentials; use the username/password settings");
    }
    if (parsed.getQuery() != null || parsed.getFragment() != null) {
      throw new IllegalArgumentException(
          "elasticsearch.clusters."
              + clusterName
              + ".uri must not contain a query string or fragment: "
              + uri);
    }

    boolean ssl = "https".equals(scheme);
    int port = parsed.getPort() >= 0 ? parsed.getPort() : (ssl ? 443 : 80);
    return new SearchClusterUri(
        scheme, ssl, parsed.getHost(), port, normalizePathPrefix(parsed.getPath()));
  }

  /**
   * Builds a URI from the legacy {@code ELASTICSEARCH_HOST} / {@code PORT} / {@code USE_SSL} /
   * {@code PATH_PREFIX} variables so existing Compose and Cloud deployments keep working without
   * setting {@code ELASTICSEARCH_URI}.
   */
  @Nonnull
  public static String synthesize(
      @Nullable String host, int port, boolean useSSL, @Nullable String pathPrefix) {
    String effectiveHost = (host == null || host.trim().isEmpty()) ? "localhost" : host.trim();
    return (useSSL ? "https" : "http")
        + "://"
        + effectiveHost
        + ":"
        + port
        + normalizePathPrefix(pathPrefix);
  }

  /**
   * Normalizes to what RestClient's {@code setPathPrefix} expects: leading slash, no trailing
   * slash. A blank path or a bare {@code /} yields {@code ""} (no prefix).
   */
  @Nonnull
  public static String normalizePathPrefix(@Nullable String path) {
    if (path == null) {
      return "";
    }
    String trimmed = path.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    if (trimmed.isEmpty()) {
      return "";
    }
    return trimmed.startsWith("/") ? trimmed : "/" + trimmed;
  }
}
