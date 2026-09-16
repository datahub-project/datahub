package com.linkedin.metadata.kafka.hook.spring;

import com.linkedin.metadata.entity.EntityService;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

public class MCLSpringGMSTestConfiguration {

  @Bean
  @Primary
  @SuppressWarnings("unchecked")
  public EntityService<?> entityService() {
    return Mockito.mock(EntityService.class);
  }
}
