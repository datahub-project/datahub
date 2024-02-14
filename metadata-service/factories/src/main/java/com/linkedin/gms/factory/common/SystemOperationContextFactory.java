package com.linkedin.gms.factory.common;

import com.datahub.authentication.Authentication;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SystemOperationContextFactory {

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Bean(name = "systemOperationContext")
  @Nonnull
  protected OperationContext systemOperationContext(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull @Qualifier("systemAuthentication") final Authentication systemAuthentication,
      @Nonnull final OperationContextConfig operationContextConfig) {

    return OperationContext.asSystem(
        operationContextConfig,
        entityRegistry,
        systemAuthentication,
        components.getIndexConvention());
  }

  @Bean
  @Nonnull
  protected OperationContextConfig operationContextConfig(
      final ConfigurationProvider configurationProvider) {
    return OperationContextConfig.builder()
        .searchAuthorizationConfiguration(configurationProvider.getAuthorization().getSearch())
        .build();
  }
}
