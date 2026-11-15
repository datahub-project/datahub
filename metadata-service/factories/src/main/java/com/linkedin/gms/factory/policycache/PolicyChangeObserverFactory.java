package com.linkedin.gms.factory.policycache;

import com.datahub.authorization.DataHubAuthorizer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.PolicyChangeObserver;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Makes a client's committed policy write update this GMS's policy cache before the write returns.
 *
 * <p>GMS scans this package (CommonApplicationConfig), and so does system-update, which has no
 * authorizer and so registers nothing. The standalone MCE consumer does not scan it: its queued
 * writes must not wait for a cache read. Its own Rest.li endpoints keep the timer refresh.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(
    name = "authorization.defaultAuthorizer.immediateUpdate.enabled",
    havingValue = "true")
public class PolicyChangeObserverFactory {

  @Bean(name = "policyChangeObserver")
  @Nullable
  protected PolicyChangeObserver policyChangeObserver(
      @Qualifier("dataHubAuthorizer") final ObjectProvider<DataHubAuthorizer> authorizerProvider,
      @Qualifier("entityService") final ObjectProvider<EntityService<?>> entityServiceProvider,
      @Value("${authorization.defaultAuthorizer.immediateUpdate.policyChangeWaitMillis:5000}")
          final long policyChangeWaitMillis) {
    // system-update scans all factory packages but builds no authorizer.
    final DataHubAuthorizer authorizer = authorizerProvider.getIfAvailable();
    if (authorizer == null || !authorizer.isImmediateUpdateEnabled()) {
      return null;
    }
    final EntityService<?> service = entityServiceProvider.getIfAvailable();
    if (!(service instanceof EntityServiceImpl impl)) {
      log.warn(
          "Policy cache: no EntityServiceImpl in this process, so policy writes here will not"
              + " update the cache before they return.");
      return null;
    }
    authorizer.setPolicyChangeWaitMillis(policyChangeWaitMillis);
    final PolicyChangeObserver observer = authorizer::onPolicyChange;
    impl.setPolicyChangeObserver(observer);
    authorizer.setPolicyWritesObserved(true);
    return observer;
  }
}
