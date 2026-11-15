package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.SearchPolicyLoader;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
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
  // bean
  // creation is blocked for at most about 60 s.
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

  @Value("${authorization.defaultAuthorizer.immediateUpdate.policyChangeWaitMillis:5000}")
  private Long policyChangeWaitMillis;

  @Bean(name = "dataHubAuthorizer")
  @Scope("singleton")
  @Nonnull
  protected DataHubAuthorizer dataHubAuthorizer(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient,
      @Qualifier("groupService") final GroupService groupService,
      @Qualifier("entityService") final ObjectProvider<EntityService<?>> entityService) {

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
      authorizer.setPolicyChangeWaitMillis(policyChangeWaitMillis);
      // Reload this process's cache after each committed policy write or delete, before the write
      // returns. Covers every synchronous write path in this process, not only GraphQL.
      final EntityService<?> service = entityService.getIfAvailable();
      if (service instanceof EntityServiceImpl impl) {
        impl.setPolicyChangeObserver(authorizer::onPolicyChange);
        authorizer.setPolicyWritesObserved(true);
      } else {
        log.warn(
            "Policy cache: no EntityServiceImpl in this process, so policy writes here will not"
                + " refresh the cache before they return.");
      }
      authorizer.initialLoad(INITIAL_LOAD_ATTEMPTS, INITIAL_LOAD_BACKOFF_MILLIS);
    } else {
      log.info("policy cache source=search");
    }
    return authorizer;
  }
}
