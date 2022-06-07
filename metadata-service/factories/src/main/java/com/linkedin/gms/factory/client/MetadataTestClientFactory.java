package com.linkedin.gms.factory.client;

import com.linkedin.restli.client.Client;
import com.linkedin.test.MetadataTestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class MetadataTestClientFactory {
  @Autowired
  @Qualifier("restliClient")
  private Client restliClient;

  @Bean("metadataTestClient")
  public MetadataTestClient getMetadataTestClient() {
    return new MetadataTestClient(restliClient);
  }
}
