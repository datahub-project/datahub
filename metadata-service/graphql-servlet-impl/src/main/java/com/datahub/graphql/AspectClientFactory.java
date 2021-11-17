package com.datahub.graphql;

import com.linkedin.entity.client.AspectClient;
import com.linkedin.gms.factory.entity.JavaEntityClientFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Deprecated, use {@link JavaEntityClientFactory} or {@link RestliEntityClientFactory} instead.
 */
@Deprecated
@Configuration
public class AspectClientFactory {

  @Value("${DATAHUB_GMS_HOST:localhost}")
  private String gmsHost;

  @Value("${DATAHUB_GMS_PORT:8080}")
  private int gmsPort;

  @Value("${DATAHUB_GMS_USE_SSL:false}")
  private boolean gmsUseSSL;

  @Value("${DATAHUB_GMS_SSL_PROTOCOL:#{null}}")
  private String gmsSslProtocol;

  @Bean("aspectClient")
  public AspectClient getAspectClient() {
    Client restClient = DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    return new AspectClient(restClient);
  }
}