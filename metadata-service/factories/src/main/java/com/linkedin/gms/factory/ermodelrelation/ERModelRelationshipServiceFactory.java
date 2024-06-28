package com.linkedin.gms.factory.ermodelrelation;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ERModelRelationshipService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ERModelRelationshipServiceFactory {
  @Bean(name = "erModelRelationshipService")
  @Scope("singleton")
  @Nonnull
  protected ERModelRelationshipService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient) throws Exception {
    return new ERModelRelationshipService(entityClient);
  }
}
