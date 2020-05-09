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
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import java.security.InvalidParameterException;
import java.util.Collections;


public class DefaultRestliClientFactory {

  private static final String DEFAULT_REQUEST_TIMEOUT_IN_MS = "10000";

  private DefaultRestliClientFactory() {
  }

  @Nonnull
  public static RestClient getRestLiD2Client(@Nonnull String restLiClientD2ZkHost,
                                             @Nonnull String restLiClientD2ZkPath) {
    final D2Client d2Client = new D2ClientBuilder()
            .setZkHosts(restLiClientD2ZkHost)
            .setBasePath(restLiClientD2ZkPath)
            .build();
    d2Client.start(new FutureCallback<None>());
    return new RestClient(d2Client, "d2://");
  }

  @Nonnull
  public static RestClient getRestLiClient(@Nonnull String restLiServerHost, int restLiServerPort) {
    if (StringUtils.isBlank(restLiServerHost) || restLiServerPort <= 0) {
      throw new InvalidParameterException("Invalid restli server host name or port!");
    }

    HttpClientFactory http = new HttpClientFactory.Builder().build();
    TransportClient transportClient = http
            .getClient(Collections.singletonMap(HttpClientFactory.HTTP_REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT_IN_MS));
    Client r2Client = new TransportClientAdapter(transportClient);
    return new RestClient(r2Client, "http://" + restLiServerHost + ":" + restLiServerPort + "/");
  }
}
