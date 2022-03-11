package com.linkedin.metadata.kafka.config;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.metadata.kafka.hydrator.EntityHydrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({RestliEntityClientFactory.class, SystemAuthenticationFactory.class})
public class EntityHydratorConfig {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _systemAuthentication;

  @Autowired
  @Qualifier("restliEntityClient")
  private RestliEntityClient _entityClient;

  @Bean
  public EntityHydrator getEntityHydrator() {
    return new EntityHydrator(_systemAuthentication, _entityClient);
  }
}
