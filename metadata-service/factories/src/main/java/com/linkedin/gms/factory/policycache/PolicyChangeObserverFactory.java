package com.linkedin.gms.factory.policycache;

import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.PolicyChangeMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.PolicyChangeObserver;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Makes each committed policy write in this GMS update its policy cache (a client write waits for
 * it before it returns), and with POLICY_CACHE_BROADCAST_ENABLED signals the change to the other
 * GMS members through Hazelcast.
 *
 * <p>GMS scans this package (CommonApplicationConfig), and so does system-update, which has no
 * authorizer and so registers nothing. The standalone MCE consumer does not scan it, so the writes
 * it commits are not signalled: other GMS members read them on their refresh timer. Its own Rest.li
 * endpoints keep the timer refresh too.
 */
@Slf4j
@Configuration
public class PolicyChangeObserverFactory {

  @Bean(name = "policyChangeObserver")
  @Nullable
  protected PolicyChangeObserver policyChangeObserver(
      @Qualifier("dataHubAuthorizer") final ObjectProvider<DataHubAuthorizer> authorizerProvider,
      @Qualifier("entityService") final ObjectProvider<EntityService<?>> entityServiceProvider,
      @Qualifier("hazelcastInstance") final ObjectProvider<HazelcastInstance> hazelcastProvider,
      @Value("${authorization.defaultAuthorizer.immediateUpdate.policyChangeWaitMillis:5000}")
          final long policyChangeWaitMillis,
      @Value("${authorization.defaultAuthorizer.changeBroadcast.enabled:false}")
          final boolean changeBroadcastEnabled) {
    // system-update scans all factory packages but builds no authorizer. The authorizer has
    // immediate updates with either POLICY_CACHE_IMMEDIATE_UPDATE_ENABLED or the broadcast flag.
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
    IMap<String, Long> changes = null;
    if (changeBroadcastEnabled) {
      final HazelcastInstance hazelcast = hazelcastProvider.getIfAvailable();
      if (hazelcast == null) {
        log.warn(
            "POLICY_CACHE_BROADCAST_ENABLED=true, but this process has no Hazelcast instance;"
                + " policy changes made here are not signalled to other GMS members.");
      } else {
        changes = PolicyChangeMap.get(hazelcast);
      }
    }
    authorizer.setPolicyChangeWaitMillis(policyChangeWaitMillis);
    final PolicyChangeObserver observer =
        new ClusterPolicyChangeObserver(
            authorizer, changes, authorizer.getSystemOpContext().getMetricUtils().orElse(null));
    impl.setPolicyChangeObserver(observer);
    authorizer.setPolicyWritesObserved(true);
    return observer;
  }
}
