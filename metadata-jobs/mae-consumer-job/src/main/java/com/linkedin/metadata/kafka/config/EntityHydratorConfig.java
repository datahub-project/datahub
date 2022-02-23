package com.linkedin.metadata.kafka.config;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.kafka.hydrator.EntityHydrator;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class EntityHydratorConfig {

  @Value("${GMS_HOST:localhost}")
  private String gmsHost;
  @Value("${GMS_PORT:8080}")
  private int gmsPort;
  @Value("${GMS_USE_SSL:false}")
  private boolean gmsUseSSL;
  @Value("${GMS_SSL_PROTOCOL:#{null}}")
  private String gmsSslProtocol;
  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Bean
  public EntityHydrator getEntityHydrator() {
    Client restClient = DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    RestliEntityClient entityClient = new RestliEntityClient(restClient);
    return new EntityHydrator(this.systemAuthentication, entityClient);
  }
}
