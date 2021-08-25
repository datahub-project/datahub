package com.datahub.metadata.graphql;

import com.datahub.metadata.authorization.AuthorizationManager;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;


// TODO: move this to gms factories module.
@Configuration
@Import({EntityClientFactory.class, AspectClientFactory.class})
public class AuthorizationManagerFactory {

  @Autowired
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("aspectClient")
  private AspectClient aspectClient;

  @Value("${POLICY_CACHE_REFRESH_INTERVAL_SECONDS:120}")
  private Integer policyCacheRefreshIntervalSeconds;

  @Bean(name = "authorizationManager")
  @Nonnull
  protected AuthorizationManager getInstance() {
    return new AuthorizationManager(entityClient, aspectClient, policyCacheRefreshIntervalSeconds);
  }
}