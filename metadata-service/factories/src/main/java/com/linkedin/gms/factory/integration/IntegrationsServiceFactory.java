package com.linkedin.gms.factory.integration;

import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.IntegrationsServiceConfiguration;
import com.linkedin.metadata.integration.IntegrationsService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SystemAuthenticationFactory.class})
public class IntegrationsServiceFactory {

  @Autowired private ConfigurationProvider _configProvider;

  @Bean(name = "integrationsService")
  @Nonnull
  protected IntegrationsService getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext)
      throws Exception {
    final IntegrationsServiceConfiguration config = _configProvider.getIntegrationsService();
    return new IntegrationsService(config.host, config.port, config.useSsl, systemOperationContext);
  }
}
