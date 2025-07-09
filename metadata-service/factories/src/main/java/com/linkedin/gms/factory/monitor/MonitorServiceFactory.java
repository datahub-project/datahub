package com.linkedin.gms.factory.monitor;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MonitorServiceConfiguration;
import com.linkedin.metadata.service.MonitorService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MonitorServiceFactory {
  @Autowired private ConfigurationProvider _configProvider;

  @Bean(name = "monitorService")
  @Nonnull
  protected MonitorService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("openApiClient") OpenApiClient openApiClient,
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext)
      throws Exception {
    final MonitorServiceConfiguration config = _configProvider.getMonitorService();
    return new MonitorService(
        config.host,
        config.port,
        config.useSsl,
        systemEntityClient,
        openApiClient,
        systemOperationContext);
  }
}
