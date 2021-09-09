package com.datahub.metadata.graphql;

import com.datahub.metadata.authorization.AuthorizationManager;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.OwnershipClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;


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

  @Value("${AUTH_POLICIES_ENABLED:true}")
  private Boolean policiesEnabled;

  @Bean(name = "authorizationManager")
  @Scope("singleton")
  @Nonnull
  protected AuthorizationManager getInstance() {

    final AuthorizationManager.AuthorizationMode mode = policiesEnabled
        ? AuthorizationManager.AuthorizationMode.DEFAULT
        : AuthorizationManager.AuthorizationMode.ALLOW_ALL;

    final OwnershipClient ownershipClient = new OwnershipClient(aspectClient);

    return new AuthorizationManager(entityClient, ownershipClient, 10, policyCacheRefreshIntervalSeconds, mode);
  }
}