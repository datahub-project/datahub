package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.SearchPolicyLoader;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
public class DataHubAuthorizerFactory {

  // Startup load: try a few times, then continue and retry in the background, so a short search or
  // database problem does not stop the process from starting. All attempts share one 60 s limit, so
  // bean creation is blocked for at most about 60 s.
  private static final int INITIAL_LOAD_ATTEMPTS = 3;
  private static final long INITIAL_LOAD_BACKOFF_MILLIS = 2000;

  @Value("${authorization.defaultAuthorizer.cacheRefreshIntervalSecs}")
  private Integer policyCacheRefreshIntervalSeconds;

  @Value("${authorization.defaultAuthorizer.cachePolicyFetchSize}")
  private Integer policyCacheFetchSize;

  @Value("${authorization.defaultAuthorizer.enabled:true}")
  private Boolean policiesEnabled;

  @Value("${authorization.defaultAuthorizer.immediateUpdate.enabled:false}")
  private Boolean immediateUpdateEnabled;

  @Value("${authorization.defaultAuthorizer.immediateUpdate.recentChangeWindowSeconds:600}")
  private Long recentChangeWindowSeconds;

  @Bean(name = "dataHubAuthorizer")
  @Scope("singleton")
  @Nonnull
  protected DataHubAuthorizer dataHubAuthorizer(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient,
      @Qualifier("groupService") final GroupService groupService) {

    final DataHubAuthorizer.AuthorizationMode mode =
        policiesEnabled
            ? DataHubAuthorizer.AuthorizationMode.DEFAULT
            : DataHubAuthorizer.AuthorizationMode.ALLOW_ALL;

    final DataHubAuthorizer authorizer =
        new DataHubAuthorizer(
            systemOpContext,
            systemEntityClient,
            groupService,
            10,
            policyCacheRefreshIntervalSeconds,
            mode,
            policyCacheFetchSize,
            Boolean.TRUE.equals(immediateUpdateEnabled)
                ? new SearchPolicyLoader(systemOpContext, systemEntityClient, policyCacheFetchSize)
                : null);

    if (authorizer.isImmediateUpdateEnabled()) {
      authorizer.setRecentChangeWindow(Duration.ofSeconds(recentChangeWindowSeconds));
      authorizer.initialLoad(INITIAL_LOAD_ATTEMPTS, INITIAL_LOAD_BACKOFF_MILLIS);
    } else {
      log.info("policy cache immediate updates off; refreshing from search on the timer");
    }
    return authorizer;
  }
}
