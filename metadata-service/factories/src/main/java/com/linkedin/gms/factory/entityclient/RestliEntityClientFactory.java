package com.linkedin.gms.factory.entityclient;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.restli.RestliClientSslConfig;
import com.linkedin.metadata.utils.BasePathUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.restli.client.Client;
import java.net.URI;
import javax.inject.Singleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The Java Entity Client should be preferred if executing within the GMS service. */
@Configuration
@ConditionalOnProperty(name = "entityClient.impl", havingValue = "restli")
public class RestliEntityClientFactory {

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

  @Bean("entityClient")
  @Singleton
  public EntityClient entityClient(
      @Value("${datahub.gms.host}") String gmsHost,
      @Value("${datahub.gms.port}") int gmsPort,
      @Value("${datahub.gms.basePath:}") String gmsBasePath,
      @Value("${datahub.gms.basePathEnabled:false}") boolean gmsBasePathEnabled,
      @Value("${datahub.gms.useSSL}") boolean gmsUseSSL,
      @Value("${datahub.gms.uri}") String gmsUri,
      @Value("${datahub.gms.sslContext.protocol}") String gmsSslProtocol,
      final EntityClientConfig entityClientConfig,
      final MetricUtils metricUtils) {
    final Client restClient;
    RestliClientSslConfig sslConfig = buildSslConfig();
    if (gmsUri != null) {
      restClient =
          DefaultRestliClientFactory.getRestLiClient(URI.create(gmsUri), gmsSslProtocol, sslConfig);
    } else {
      // Use the same logic as GMSConfiguration.getResolvedBasePath()
      String resolvedBasePath = BasePathUtils.resolveBasePath(gmsBasePathEnabled, gmsBasePath);
      restClient =
          DefaultRestliClientFactory.getRestLiClient(
              gmsHost, gmsPort, resolvedBasePath, gmsUseSSL, gmsSslProtocol, null, sslConfig);
    }
    return new RestliEntityClient(restClient, entityClientConfig, metricUtils);
  }

  @Bean("systemEntityClient")
  @Singleton
  public SystemEntityClient systemEntityClient(
      @Value("${datahub.gms.host}") String gmsHost,
      @Value("${datahub.gms.port}") int gmsPort,
      @Value("${datahub.gms.basePath:}") String gmsBasePath,
      @Value("${datahub.gms.basePathEnabled:false}") boolean gmsBasePathEnabled,
      @Value("${datahub.gms.useSSL}") boolean gmsUseSSL,
      @Value("${datahub.gms.uri}") String gmsUri,
      @Value("${datahub.gms.sslContext.protocol}") String gmsSslProtocol,
      final EntityClientCacheConfig entityClientCacheConfig,
      final EntityClientConfig entityClientConfig,
      final MetricUtils metricUtils) {

    final Client restClient;
    RestliClientSslConfig sslConfig = buildSslConfig();
    if (gmsUri != null) {
      restClient =
          DefaultRestliClientFactory.getRestLiClient(URI.create(gmsUri), gmsSslProtocol, sslConfig);
    } else {
      // Use the same logic as GMSConfiguration.getResolvedBasePath()
      String resolvedBasePath = BasePathUtils.resolveBasePath(gmsBasePathEnabled, gmsBasePath);
      restClient =
          DefaultRestliClientFactory.getRestLiClient(
              gmsHost, gmsPort, resolvedBasePath, gmsUseSSL, gmsSslProtocol, null, sslConfig);
    }
    return new SystemRestliEntityClient(
        restClient, entityClientConfig, entityClientCacheConfig, metricUtils);
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
