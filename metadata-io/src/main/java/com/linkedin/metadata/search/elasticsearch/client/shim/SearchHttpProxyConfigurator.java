package com.linkedin.metadata.search.elasticsearch.client.shim;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.ShimConfiguration;
import javax.annotation.Nonnull;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

/**
 * Applies a resolved per-cluster HTTP proxy onto the RestClient HTTP stack.
 *
 * <p>Does not call {@link HttpAsyncClientBuilder#useSystemProperties()}; JVM proxy properties are
 * interpreted earlier in {@code HttpProxySettings#resolve}.
 */
public final class SearchHttpProxyConfigurator {

  private SearchHttpProxyConfigurator() {}

  public static void apply(
      @Nonnull HttpAsyncClientBuilder builder, @Nonnull ShimConfiguration config) {
    if (config.getProxyHost() == null) {
      return;
    }
    builder.setProxy(new HttpHost(config.getProxyHost(), proxyPort(config), proxyScheme(config)));
  }

  public static void addProxyCredentials(
      @Nonnull CredentialsProvider credentialsProvider, @Nonnull ShimConfiguration config) {
    if (!hasProxyCredentials(config)) {
      return;
    }
    credentialsProvider.setCredentials(
        new AuthScope(config.getProxyHost(), proxyPort(config)),
        new UsernamePasswordCredentials(config.getProxyUsername(), config.getProxyPassword()));
  }

  public static boolean hasProxyCredentials(@Nonnull ShimConfiguration config) {
    return config.getProxyHost() != null
        && config.getProxyUsername() != null
        && config.getProxyPassword() != null;
  }

  private static int proxyPort(@Nonnull ShimConfiguration config) {
    if (config.getProxyPort() != null) {
      return config.getProxyPort();
    }
    return "https".equalsIgnoreCase(config.getProxyScheme()) ? 443 : 80;
  }

  @Nonnull
  private static String proxyScheme(@Nonnull ShimConfiguration config) {
    return config.getProxyScheme() == null ? "http" : config.getProxyScheme();
  }
}
