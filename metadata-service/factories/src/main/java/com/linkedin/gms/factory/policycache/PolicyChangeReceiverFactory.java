package com.linkedin.gms.factory.policycache;

import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.PolicyChangeMap;
import com.hazelcast.core.HazelcastInstance;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Starts a {@link PolicyChangeReceiver} in GMS when POLICY_CACHE_BROADCAST_ENABLED is true.
 *
 * <p>GMS scans this package (CommonApplicationConfig) and always runs a Hazelcast member. system-
 * update scans it too but has no authorizer, so it starts nothing. The standalone MCE consumer does
 * not scan it.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(
    name = "authorization.defaultAuthorizer.changeBroadcast.enabled",
    havingValue = "true")
public class PolicyChangeReceiverFactory {

  @Bean(name = "policyChangeReceiver", destroyMethod = "close")
  @Nullable
  protected PolicyChangeReceiver policyChangeReceiver(
      @Qualifier("dataHubAuthorizer") final ObjectProvider<DataHubAuthorizer> authorizerProvider,
      @Qualifier("hazelcastInstance") final ObjectProvider<HazelcastInstance> hazelcastProvider) {
    final DataHubAuthorizer authorizer = authorizerProvider.getIfAvailable();
    if (authorizer == null || !authorizer.isImmediateUpdateEnabled()) {
      return null;
    }
    final HazelcastInstance hazelcast = hazelcastProvider.getIfAvailable();
    if (hazelcast == null) {
      log.warn(
          "Policy change receiver not started: no Hazelcast instance in this process; policy"
              + " changes from other GMS members are read on the refresh timer.");
      return null;
    }
    final PolicyChangeReceiver receiver =
        new PolicyChangeReceiver(
            authorizer,
            PolicyChangeMap.get(hazelcast),
            authorizer.getSystemOpContext().getMetricUtils().orElse(null));
    receiver.start();
    return receiver;
  }
}
