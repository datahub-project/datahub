package com.linkedin.metadata.config.search;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

/**
 * Per-cluster HTTP proxy ({@code elasticsearch.clusters.<name>.proxy}).
 *
 * <p>An explicit {@link #host} always wins. When host is blank and {@link
 * #isUseSystemProxyProperties()} is true, only the JRE <em>proxy</em> system properties are read
 * ({@code http.proxyHost}/{@code Port}, {@code https.proxyHost}/{@code Port}, {@code
 * http.nonProxyHosts}). Apache {@code HttpAsyncClientBuilder#useSystemProperties()} is never used,
 * so unrelated JVM HTTP/SSL knobs are ignored.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class HttpProxySettings {
  private String host;
  private Integer port;
  private String scheme;
  private String username;
  private String password;

  /**
   * When {@link #host} is blank, honor JVM proxy system properties. Null or unset means true so
   * existing {@code -Dhttp.proxyHost} deployments keep working without a yaml block.
   */
  private Boolean useSystemProxyProperties;

  public String getHost() {
    return SearchClusterSettings.blankToNull(host);
  }

  public String getScheme() {
    return SearchClusterSettings.blankToNull(scheme);
  }

  public String getUsername() {
    return SearchClusterSettings.blankToNull(username);
  }

  public String getPassword() {
    return SearchClusterSettings.blankToNull(password);
  }

  public boolean isUseSystemProxyProperties() {
    return useSystemProxyProperties == null || useSystemProxyProperties;
  }

  /**
   * Resolved proxy for one cluster endpoint, or empty when the client should connect directly.
   *
   * @param clusterHost hostname of the search cluster (used for {@code http.nonProxyHosts})
   * @param clusterUseSsl whether the cluster URI is https (selects http vs https proxy properties)
   */
  @Nonnull
  public static Optional<ResolvedHttpProxy> resolve(
      @Nullable HttpProxySettings settings, @Nullable String clusterHost, boolean clusterUseSsl) {
    return resolve(settings, clusterHost, clusterUseSsl, System::getProperty);
  }

  @Nonnull
  static Optional<ResolvedHttpProxy> resolve(
      @Nullable HttpProxySettings settings,
      @Nullable String clusterHost,
      boolean clusterUseSsl,
      @Nonnull Function<String, String> systemProperties) {
    if (settings != null && settings.getHost() != null) {
      String scheme = settings.getScheme() == null ? "http" : settings.getScheme().toLowerCase();
      int port =
          settings.getPort() != null ? settings.getPort() : ("https".equals(scheme) ? 443 : 80);
      return Optional.of(
          new ResolvedHttpProxy(
              settings.getHost(), port, scheme, settings.getUsername(), settings.getPassword()));
    }
    if (settings != null && !settings.isUseSystemProxyProperties()) {
      return Optional.empty();
    }
    if (matchesNonProxyHosts(clusterHost, systemProperties.apply("http.nonProxyHosts"))) {
      return Optional.empty();
    }
    String proxyHost;
    String portProperty;
    if (clusterUseSsl) {
      proxyHost = blankToNull(systemProperties.apply("https.proxyHost"));
      portProperty = "https.proxyPort";
      if (proxyHost == null) {
        proxyHost = blankToNull(systemProperties.apply("http.proxyHost"));
        portProperty = "http.proxyPort";
      }
    } else {
      proxyHost = blankToNull(systemProperties.apply("http.proxyHost"));
      portProperty = "http.proxyPort";
    }
    if (proxyHost == null) {
      return Optional.empty();
    }
    int defaultPort = "https.proxyPort".equals(portProperty) ? 443 : 80;
    return Optional.of(
        new ResolvedHttpProxy(
            proxyHost,
            parsePort(systemProperties.apply(portProperty), defaultPort),
            "http",
            null,
            null));
  }

  static boolean matchesNonProxyHosts(@Nullable String host, @Nullable String nonProxyHosts) {
    if (host == null || host.isEmpty() || nonProxyHosts == null || nonProxyHosts.isEmpty()) {
      return false;
    }
    String normalizedHost = host.toLowerCase(Locale.ROOT);
    for (String rawPattern : nonProxyHosts.split("\\|")) {
      String pattern = rawPattern.trim();
      if (pattern.isEmpty()) {
        continue;
      }
      try {
        Pattern compiled = Pattern.compile(wildcardToRegex(pattern), Pattern.CASE_INSENSITIVE);
        if (compiled.matcher(normalizedHost).matches()) {
          return true;
        }
      } catch (PatternSyntaxException ignored) {
        if (pattern.equalsIgnoreCase(host)) {
          return true;
        }
      }
    }
    return false;
  }

  @Nonnull
  private static String wildcardToRegex(@Nonnull String glob) {
    StringBuilder regex = new StringBuilder();
    for (int i = 0; i < glob.length(); i++) {
      char c = glob.charAt(i);
      if (c == '*') {
        regex.append(".*");
      } else if (".[]{}()+-^$|\\".indexOf(c) >= 0) {
        regex.append('\\').append(c);
      } else {
        regex.append(c);
      }
    }
    return regex.toString();
  }

  private static int parsePort(@Nullable String raw, int defaultPort) {
    if (raw == null || raw.trim().isEmpty()) {
      return defaultPort;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      return defaultPort;
    }
  }

  @Nullable
  private static String blankToNull(@Nullable String value) {
    return SearchClusterSettings.blankToNull(value);
  }

  @Value
  public static class ResolvedHttpProxy {
    String host;
    int port;
    String scheme;
    String username;
    String password;
  }
}
