package com.linkedin.gms.factory.ermodelrelation;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ERModelRelationshipService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ERModelRelationshipServiceFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "erModelRelationshipService")
  @Scope("singleton")
  @Nonnull
  protected ERModelRelationshipService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new ERModelRelationshipService(entityClient, _authentication);
  }
}
