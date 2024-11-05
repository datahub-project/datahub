package com.linkedin.gms.factory.dimension;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.DimensionTypeService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DimensionTypeServiceFactory {

  @Bean(name = "dimensionTypeService")
  @Scope("singleton")
  @Nonnull
  protected DimensionTypeService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient)
      throws Exception {
    return new DimensionTypeService(systemEntityClient);
  }
}
