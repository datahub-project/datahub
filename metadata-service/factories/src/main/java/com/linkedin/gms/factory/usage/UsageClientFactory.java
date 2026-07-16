package com.linkedin.gms.factory.usage;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.restli.RestliClientSslConfig;
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

  @Value("${datahub.gms.host:localhost}")
  private String gmsHost;

  @Value("${datahub.gms.port:8080}")
  private int gmsPort;

  @Value("${datahub.gms.useSSL:false}")
  private boolean gmsUseSSL;

  @Value("${datahub.gms.sslContext.protocol:#{null}}")
  private String gmsSslProtocol;

  @Value("${datahub.gms.truststore.path:#{null}}")
  private String truststorePath;

  @Value("${datahub.gms.truststore.password:#{null}}")
  private String truststorePassword;

  @Value("${datahub.gms.truststore.type:#{null}}")
  private String truststoreType;

  @Value("${datahub.gms.keystore.path:#{null}}")
  private String keystorePath;

  @Value("${datahub.gms.keystore.password:#{null}}")
  private String keystorePassword;

  @Value("${datahub.gms.keystore.type:#{null}}")
  private String keystoreType;

  @Value("${datahub.gms.keystore.keyPassword:#{null}}")
  private String keyPassword;

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

    RestliClientSslConfig sslConfig = buildSslConfig();
    Client restClient =
        DefaultRestliClientFactory.getRestLiClient(
            gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol, params, sslConfig);
    return new RestliUsageClient(
        restClient,
        new ExponentialBackoff(retryInterval),
        numRetries,
        configurationProvider.getCache().getClient().getUsageClient(),
        metricUtils);
  }

  private RestliClientSslConfig buildSslConfig() {
    return RestliClientSslConfig.fromNullableStrings(
        truststorePath,
        truststorePassword,
        truststoreType,
        keystorePath,
        keystorePassword,
        keystoreType,
        keyPassword);
  }
}
