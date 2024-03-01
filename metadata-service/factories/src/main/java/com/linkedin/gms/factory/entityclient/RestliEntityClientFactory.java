package com.linkedin.gms.factory.entityclient;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import javax.inject.Singleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/** The Java Entity Client should be preferred if executing within the GMS service. */
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
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
      @Value("${entityClient.retryInterval:2}") int retryInterval,
      @Value("${entityClient.numRetries:3}") int numRetries) {
    final Client restClient;
    if (gmsUri != null) {
      restClient = DefaultRestliClientFactory.getRestLiClient(URI.create(gmsUri), gmsSslProtocol);
    } else {
      restClient =
          DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    }
    return new RestliEntityClient(restClient, new ExponentialBackoff(retryInterval), numRetries);
  }

  @Bean("systemEntityClient")
  @Singleton
  public SystemEntityClient systemEntityClient(
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      @Value("${datahub.gms.host}") String gmsHost,
      @Value("${datahub.gms.port}") int gmsPort,
      @Value("${datahub.gms.useSSL}") boolean gmsUseSSL,
      @Value("${datahub.gms.uri}") String gmsUri,
      @Value("${datahub.gms.sslContext.protocol}") String gmsSslProtocol,
      @Value("${entityClient.retryInterval:2}") int retryInterval,
      @Value("${entityClient.numRetries:3}") int numRetries,
      final EntityClientCacheConfig entityClientCacheConfig) {

    final Client restClient;
    if (gmsUri != null) {
      restClient = DefaultRestliClientFactory.getRestLiClient(URI.create(gmsUri), gmsSslProtocol);
    } else {
      restClient =
          DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    }
    return new SystemRestliEntityClient(
        systemOperationContext,
        restClient,
        new ExponentialBackoff(retryInterval),
        numRetries,
        entityClientCacheConfig);
  }
}
