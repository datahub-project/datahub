package com.linkedin.gms.factory.usage;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.Client;
import com.linkedin.usage.RestliUsageClient;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UsageClientFactory {

  @Value("${DATAHUB_GMS_HOST:localhost}")
  private String gmsHost;

  @Value("${DATAHUB_GMS_PORT:8080}")
  private int gmsPort;

  @Value("${DATAHUB_GMS_USE_SSL:false}")
  private boolean gmsUseSSL;

  @Value("${DATAHUB_GMS_SSL_PROTOCOL:#{null}}")
  private String gmsSslProtocol;

  @Value("${usageClient.retryInterval:2}")
  private int retryInterval;

  @Value("${usageClient.numRetries:0}")
  private int numRetries;

  @Value("${usageClient.timeoutMs:3000}")
  private long timeoutMs;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configurationProvider;

  @Bean("usageClient")
  public RestliUsageClient getUsageClient(MetricUtils metricUtils) {
    Map<String, String> params = new HashMap<>();
    params.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, String.valueOf(timeoutMs));

    Client restClient =
        DefaultRestliClientFactory.getRestLiClient(
            gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol, params);
    return new RestliUsageClient(
        restClient,
        new ExponentialBackoff(retryInterval),
        numRetries,
        configurationProvider.getCache().getClient().getUsageClient(),
        metricUtils);
  }
}
