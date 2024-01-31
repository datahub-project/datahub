package com.linkedin.gms.factory.auth;

import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class DataHubAuthorizerFactory {

  @Value("${authorization.defaultAuthorizer.cacheRefreshIntervalSecs}")
  private Integer policyCacheRefreshIntervalSeconds;

  @Value("${authorization.defaultAuthorizer.cachePolicyFetchSize}")
  private Integer policyCacheFetchSize;

  @Value("${authorization.defaultAuthorizer.enabled:true}")
  private Boolean policiesEnabled;

  @Bean(name = "dataHubAuthorizer")
  @Scope("singleton")
  @Nonnull
  protected DataHubAuthorizer dataHubAuthorizer(final SystemEntityClient systemEntityClient) {

    final DataHubAuthorizer.AuthorizationMode mode =
        policiesEnabled
            ? DataHubAuthorizer.AuthorizationMode.DEFAULT
            : DataHubAuthorizer.AuthorizationMode.ALLOW_ALL;

    return new DataHubAuthorizer(
        systemEntityClient.getSystemAuthentication(),
        systemEntityClient,
        10,
        policyCacheRefreshIntervalSeconds,
        mode,
        policyCacheFetchSize);
  }
}
