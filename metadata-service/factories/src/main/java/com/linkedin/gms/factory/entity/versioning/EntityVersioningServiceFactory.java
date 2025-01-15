package com.linkedin.gms.factory.entity.versioning;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.EntityVersioningServiceImpl;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class EntityVersioningServiceFactory {

  @Bean(name = "entityVersioningService")
  @Nonnull
  protected EntityVersioningService createInstance(EntityService<?> entityService) {

    return new EntityVersioningServiceImpl(entityService);
  }
}
