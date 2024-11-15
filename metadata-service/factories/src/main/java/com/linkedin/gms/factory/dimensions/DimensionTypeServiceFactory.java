package com.linkedin.gms.factory.dimensions;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.DimensionTypeService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DimensionTypeServiceFactory {

  @Bean(name = "dimensionTypeService")
  @Scope("singleton")
  @Nonnull
  protected DimensionTypeService getInstance(final SystemEntityClient systemEntityClient)
      throws Exception {
    return new DimensionTypeService(systemEntityClient);
  }
}
