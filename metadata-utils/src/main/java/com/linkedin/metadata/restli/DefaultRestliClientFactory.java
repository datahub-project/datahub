package com.linkedin.metadata.restli;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import java.net.URI;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.apache.commons.lang.StringUtils;

public class DefaultRestliClientFactory {

  private static final String DEFAULT_REQUEST_TIMEOUT_IN_MS = "10000";

  private DefaultRestliClientFactory() {}

  @Nonnull
  public static RestClient getRestLiD2Client(
      @Nonnull String restLiClientD2ZkHost, @Nonnull String restLiClientD2ZkPath) {
    final D2Client d2Client =
        new D2ClientBuilder()
            .setZkHosts(restLiClientD2ZkHost)
            .setBasePath(restLiClientD2ZkPath)
            .build();
    d2Client.start(new FutureCallback<None>());
    return new RestClient(d2Client, "d2://");
  }

  @Nonnull
  public static RestClient getRestLiClient(
      @Nonnull String restLiServerHost,
      int restLiServerPort,
      boolean useSSL,
      @Nullable String sslProtocol) {
    return getRestLiClient(restLiServerHost, restLiServerPort, useSSL, sslProtocol, null);
  }

  @Nonnull
  public static RestClient getRestLiClient(
      @Nonnull String restLiServerHost,
      int restLiServerPort,
      boolean useSSL,
      @Nullable String sslProtocol,
      @Nullable Map<String, String> params) {
    return getRestLiClient(
        URI.create(
            String.format(
                "%s://%s:%s", useSSL ? "https" : "http", restLiServerHost, restLiServerPort)),
        sslProtocol,
        params);
  }

  @Nonnull
  public static RestClient getRestLiClient(@Nonnull URI gmsUri, @Nullable String sslProtocol) {
    return getRestLiClient(gmsUri, sslProtocol, null);
  }

  @Nonnull
  public static RestClient getRestLiClient(
      @Nonnull URI gmsUri,
      @Nullable String sslProtocol,
      @Nullable Map<String, String> inputParams) {
    if (StringUtils.isBlank(gmsUri.getHost()) || gmsUri.getPort() <= 0) {
      throw new InvalidParameterException("Invalid restli server host name or port!");
    }

    Map<String, Object> params = new HashMap<>();
    if (inputParams != null) {
      params.putAll(inputParams);
    }

    if ("https".equals(gmsUri.getScheme())) {
      try {
        params.put(HttpClientFactory.HTTP_SSL_CONTEXT, SSLContext.getDefault());
      } catch (NoSuchAlgorithmException ex) {
        throw new RuntimeException(ex);
      }

      SSLParameters sslParameters = new SSLParameters();
      if (sslProtocol != null) {
        sslParameters.setProtocols(new String[] {sslProtocol});
      }
      params.put(HttpClientFactory.HTTP_SSL_PARAMS, sslParameters);
    }

    return getHttpRestClient(gmsUri, params);
  }

  private static RestClient getHttpRestClient(
      @Nonnull URI gmsUri, @Nonnull Map<String, Object> params) {
    Map<String, Object> finalParams = new HashMap<>();
    finalParams.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT_IN_MS);
    finalParams.putAll(params);

    HttpClientFactory http = new HttpClientFactory.Builder().build();
    TransportClient transportClient = http.getClient(Collections.unmodifiableMap(finalParams));
    Client r2Client = new TransportClientAdapter(transportClient);
    String uriPrefix = gmsUri.getPath().endsWith("/") ? gmsUri.toString() : gmsUri + "/";
    return new RestClient(r2Client, uriPrefix);
  }
}
