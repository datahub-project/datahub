package com.linkedin.gms.factory.dataproduct;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.DataProductService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataProductServiceFactory {

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "dataProductService")
  @Scope("singleton")
  @Nonnull
  protected DataProductService getInstance(
      @Qualifier("entityClient") final EntityClient entityClient) throws Exception {
    return new DataProductService(entityClient, _graphClient);
  }
}
