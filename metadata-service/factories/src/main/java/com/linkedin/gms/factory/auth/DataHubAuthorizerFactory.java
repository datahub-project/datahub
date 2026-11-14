package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import io.datahubproject.metadata.context.OperationContext;
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

  // Startup load: try a few times, then continue and retry in the background, so a short database
  // problem does not stop the process from starting. All attempts share one 60 s limit, so bean
  // creation is blocked for at most about 60 s.
  private static final int INITIAL_LOAD_ATTEMPTS = 3;
  private static final long INITIAL_LOAD_BACKOFF_MILLIS = 2000;

  @Value("${authorization.defaultAuthorizer.cacheRefreshIntervalSecs}")
  private Integer policyCacheRefreshIntervalSeconds;

  @Value("${authorization.defaultAuthorizer.cachePolicyFetchSize}")
  private Integer policyCacheFetchSize;

  @Value("${authorization.defaultAuthorizer.enabled:true}")
  private Boolean policiesEnabled;

  @Value("${authorization.defaultAuthorizer.sqlSource.enabled:false}")
  private Boolean sqlSourceEnabled;

  @Value("${authorization.defaultAuthorizer.sqlSource.policyChangeWaitMillis:5000}")
  private Long policyChangeWaitMillis;

  @Value("${authorization.defaultAuthorizer.changeBroadcast.enabled:false}")
  private Boolean changeBroadcastEnabled;

  @Value("${datahub.messaging.transport:kafka}")
  private String messagingTransport;

  @Bean(name = "dataHubAuthorizer")
  @Scope("singleton")
  @Nonnull
  protected DataHubAuthorizer dataHubAuthorizer(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient,
      @Qualifier("groupService") final GroupService groupService,
      @Qualifier("entityAspectDao") final ObjectProvider<AspectDao> aspectDao,
      @Qualifier("entityService") final ObjectProvider<EntityService<?>> entityService) {

    final DataHubAuthorizer.AuthorizationMode mode =
        policiesEnabled
            ? DataHubAuthorizer.AuthorizationMode.DEFAULT
            : DataHubAuthorizer.AuthorizationMode.ALLOW_ALL;

    final boolean broadcast = Boolean.TRUE.equals(changeBroadcastEnabled);
    // The broadcast relies on the SQL source (ordered full reloads), so it turns it on.
    final boolean sqlSource = Boolean.TRUE.equals(sqlSourceEnabled) || broadcast;
    if (broadcast && !Boolean.TRUE.equals(sqlSourceEnabled)) {
      log.info(
          "POLICY_CACHE_BROADCAST_ENABLED=true turns on the SQL policy source"
              + " (POLICY_CACHE_SQL_SOURCE_ENABLED).");
    }
    if (broadcast && !"kafka".equalsIgnoreCase(messagingTransport)) {
      log.warn(
          "POLICY_CACHE_BROADCAST_ENABLED=true is not supported with messaging transport {} yet;"
              + " other processes refresh policies on their timer.",
          messagingTransport);
    }
    final AspectDao dao = sqlSource ? aspectDao.getIfAvailable() : null;
    if (sqlSource && dao == null) {
      log.error(
          "POLICY_CACHE_SQL_SOURCE_ENABLED is true, but this process has no entity store."
              + " Falling back to the search-based policy cache.");
    }

    final DataHubAuthorizer authorizer =
        new DataHubAuthorizer(
            systemOpContext,
            systemEntityClient,
            groupService,
            10,
            policyCacheRefreshIntervalSeconds,
            mode,
            policyCacheFetchSize,
            dao != null
                ? new SqlPolicyLoader(dao, systemOpContext.getMetricUtils().orElse(null))
                : null);

    if (authorizer.isSqlSourceEnabled()) {
      authorizer.setPolicyChangeWaitMillis(policyChangeWaitMillis);
      // Reload this process's cache after each committed policy write or delete, before the write
      // returns. Covers every synchronous write path in this process, not only GraphQL.
      final EntityService<?> service = entityService.getIfAvailable();
      if (service instanceof EntityServiceImpl impl) {
        impl.setPolicyChangeObserver(authorizer::onPolicyChange);
        authorizer.setPolicyWritesObserved(true);
        impl.setSyncPolicyWrites(broadcast);
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
