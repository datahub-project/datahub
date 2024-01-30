package com.linkedin.gms.factory.ermodelrelation;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.ERModelRelationService;
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
public class ERModelRelationServiceFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "eRModelRelationService")
  @Scope("singleton")
  @Nonnull
  protected ERModelRelationService getInstance(
          @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new ERModelRelationService(entityClient, _authentication);
  }
}
