package com.linkedin.gms.factory.dataproduct;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.DataProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

import javax.annotation.Nonnull;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class DataProductServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "dataProductService")
  @Scope("singleton")
  @Nonnull
  protected DataProductService getInstance() throws Exception {
    return new DataProductService(_javaEntityClient, _graphClient);
  }
}
