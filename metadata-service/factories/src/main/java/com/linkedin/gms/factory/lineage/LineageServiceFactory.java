package com.linkedin.gms.factory.lineage;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.LineageService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class LineageServiceFactory {

  @Bean(name = "lineageService")
  @Scope("singleton")
  @Nonnull
  protected LineageService getInstance(@Qualifier("entityClient") final EntityClient entityClient)
      throws Exception {
    return new LineageService(entityClient);
  }
}
