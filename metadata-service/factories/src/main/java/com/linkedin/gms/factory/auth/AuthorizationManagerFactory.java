package com.linkedin.gms.factory.auth;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationManager;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.entity.client.OwnershipClient;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({RestliEntityClientFactory.class})
public class AuthorizationManagerFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Autowired
  @Qualifier("javaEntityClient")
  private EntityClient entityClient;

  @Value("${authorizationManager.cacheRefreshIntervalSecs}")
  private Integer policyCacheRefreshIntervalSeconds;

  @Value("${authorizationManager.enabled:true}")
  private Boolean policiesEnabled;

  @Bean(name = "authorizationManager")
  @Scope("singleton")
  @Nonnull
  protected AuthorizationManager getInstance() {

    final AuthorizationManager.AuthorizationMode mode = policiesEnabled
        ? AuthorizationManager.AuthorizationMode.DEFAULT
        : AuthorizationManager.AuthorizationMode.ALLOW_ALL;

    final OwnershipClient ownershipClient = new OwnershipClient(entityClient);

    return new AuthorizationManager(systemAuthentication, entityClient, ownershipClient, 10, policyCacheRefreshIntervalSeconds, mode);
  }
}
