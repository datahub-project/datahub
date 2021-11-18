package com.linkedin.gms.factory.entity;

import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RestliEntityClientFactory {

  @Value("${DATAHUB_GMS_HOST:localhost}")
  private String gmsHost;

  @Value("${DATAHUB_GMS_PORT:8080}")
  private int gmsPort;

  @Value("${DATAHUB_GMS_USE_SSL:false}")
  private boolean gmsUseSSL;

  @Value("${DATAHUB_GMS_SSL_PROTOCOL:#{null}}")
  private String gmsSslProtocol;

  @Bean("restliEntityClient")
  public RestliEntityClient getRestliEntityClient() {
    Client restClient = DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort, gmsUseSSL, gmsSslProtocol);
    return new RestliEntityClient(restClient);
  }
}
