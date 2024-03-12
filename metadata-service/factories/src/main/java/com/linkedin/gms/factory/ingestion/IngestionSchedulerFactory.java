package com.linkedin.gms.factory.ingestion;

import com.datahub.authentication.Authentication;
import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Import({SystemAuthenticationFactory.class, RestliEntityClientFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class IngestionSchedulerFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _systemAuthentication;

  @Autowired
  @Qualifier("restliEntityClient")
  private RestliEntityClient _entityClient;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider _configProvider;

  @Value("${ingestion.scheduler.delayIntervalSeconds:45}") // Boot up ingestion source cache after
  // waiting 45 seconds for startup.
  private Integer _delayIntervalSeconds;

  @Value("${ingestion.scheduler.refreshIntervalSeconds:43200}") // By default, refresh ingestion
  // sources 2 times per day.
  private Integer _refreshIntervalSeconds;

  @Bean(name = "ingestionScheduler")
  @Scope("singleton")
  @Nonnull
  protected IngestionScheduler getInstance() {
    return new IngestionScheduler(
        _systemAuthentication,
        _entityClient,
        _configProvider.getIngestion(),
        _delayIntervalSeconds,
        _refreshIntervalSeconds);
  }
}
