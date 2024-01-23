package com.linkedin.gms.factory.ownership;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class OwnershipTypeServiceFactory {

  @Bean(name = "ownerShipTypeService")
  @Scope("singleton")
  @Nonnull
  protected OwnershipTypeService getInstance(final SystemEntityClient entityClient)
      throws Exception {
    return new OwnershipTypeService(entityClient, entityClient.getSystemAuthentication());
  }
}
