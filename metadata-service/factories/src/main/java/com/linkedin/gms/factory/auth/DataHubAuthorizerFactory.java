package com.linkedin.gms.factory.auth;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.AspectDao;
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

  // Startup load: try a few times, then continue and let the timer retry, so a short database
  // problem does not stop the process from starting. Worst case with the database down: 3 attempts
  // x 60 s load timeout + 2 x 2 s backoff, about 3 minutes of blocked bean creation.
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

  @Bean(name = "dataHubAuthorizer")
  @Scope("singleton")
  @Nonnull
  protected DataHubAuthorizer dataHubAuthorizer(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      final SystemEntityClient systemEntityClient,
      @Qualifier("groupService") final GroupService groupService,
      @Qualifier("entityAspectDao") final ObjectProvider<AspectDao> aspectDao) {

    final DataHubAuthorizer.AuthorizationMode mode =
        policiesEnabled
            ? DataHubAuthorizer.AuthorizationMode.DEFAULT
            : DataHubAuthorizer.AuthorizationMode.ALLOW_ALL;

    final AspectDao dao = Boolean.TRUE.equals(sqlSourceEnabled) ? aspectDao.getIfAvailable() : null;
    if (Boolean.TRUE.equals(sqlSourceEnabled) && dao == null) {
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
            dao != null ? new SqlPolicyLoader(dao) : null);

    if (authorizer.isSqlSourceEnabled()) {
      authorizer.initialLoad(INITIAL_LOAD_ATTEMPTS, INITIAL_LOAD_BACKOFF_MILLIS);
    } else {
      log.info("policy cache source=search");
    }
    return authorizer;
  }
}
