package com.linkedin.gms.factory.monitor;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MonitorServiceConfiguration;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class MonitorServiceFactory {
  @Autowired private ConfigurationProvider _configProvider;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "monitorService")
  @Scope("singleton")
  @Nonnull
  protected MonitorService getInstance(final SystemEntityClient systemEntityClient)
      throws Exception {
    final MonitorServiceConfiguration config = _configProvider.getMonitorService();
    return new MonitorService(
        config.host,
        config.port,
        config.useSsl,
        systemEntityClient,
        systemEntityClient.getSystemAuthentication());
  }
}
