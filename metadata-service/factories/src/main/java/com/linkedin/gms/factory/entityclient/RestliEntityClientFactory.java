package com.linkedin.gms.factory.entityclient;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
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

  @Bean("entityClient")
  @Singleton
  public EntityClient entityClient(
      @Value("${datahub.gms.host}") String gmsHost,
      @Value("${datahub.gms.port}") int gmsPort,
      @Value("${datahub.gms.useSSL}") boolean gmsUseSSL,
      @Value("${datahub.gms.uri}") String gmsUri,
      @Value("${datahub.gms.sslContext.protocol}") String gmsSslProtocol,
      final EntityClientConfig entityClientConfig) {
    final Client restClient;
    if (gmsUri != null) {
      restClient = DefaultRestliClientFactory.getRestLiClient(URI.create(gmsUri), gmsSslProtocol);
    } else {
      restClient =
          DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    }
    return new RestliEntityClient(restClient, entityClientConfig);
  }

  @Bean("systemEntityClient")
  @Singleton
  public SystemEntityClient systemEntityClient(
      @Value("${datahub.gms.host}") String gmsHost,
      @Value("${datahub.gms.port}") int gmsPort,
      @Value("${datahub.gms.useSSL}") boolean gmsUseSSL,
      @Value("${datahub.gms.uri}") String gmsUri,
      @Value("${datahub.gms.sslContext.protocol}") String gmsSslProtocol,
      final EntityClientCacheConfig entityClientCacheConfig,
      final EntityClientConfig entityClientConfig) {

    final Client restClient;
    if (gmsUri != null) {
      restClient = DefaultRestliClientFactory.getRestLiClient(URI.create(gmsUri), gmsSslProtocol);
    } else {
      restClient =
          DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    }
    return new SystemRestliEntityClient(restClient, entityClientConfig, entityClientCacheConfig);
  }
}
