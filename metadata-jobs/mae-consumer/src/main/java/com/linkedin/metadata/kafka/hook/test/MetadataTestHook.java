package com.linkedin.metadata.kafka.hook.test;

import com.codahale.metrics.Timer;
import com.datahub.authentication.Authentication;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.client.MetadataTestClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.MetadataTestClient;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;


/**
 * This hook evaluates tests when updates to entities come in
 * Note, it uses a cache to make sure we run tests once even if multiple update events for the given entity comes in
 */
@Slf4j
@Component
@Singleton
@Import({MetadataTestClientFactory.class, SystemAuthenticationFactory.class, EntityRegistryFactory.class})
public class MetadataTestHook implements MetadataChangeLogHook {

  private final EntityRegistry _entityRegistry;
  private final MetadataTestClient _testClient;
  private final Authentication _systemAuthentication;
  private final Cache<Urn, Long> _urnObserverCache;

  @Autowired
  public MetadataTestHook(@Nonnull final EntityRegistry entityRegistry, @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication) {
    _entityRegistry = entityRegistry;
    _testClient = testClient;
    _systemAuthentication = systemAuthentication;
    _urnObserverCache = CacheBuilder.newBuilder()
        .expireAfterWrite(2, TimeUnit.SECONDS)
        .removalListener(RemovalListeners.asynchronous(
            (RemovalListener<Urn, Long>) removalNotification -> {
              if (removalNotification.getCause() == RemovalCause.EXPIRED) {
                evaluateTest(removalNotification.getKey());
              }
            },
            Executors.newCachedThreadPool()))
        .build();
    ScheduledExecutorService cleanUpService = Executors.newScheduledThreadPool(1);
    cleanUpService.scheduleAtFixedRate(_urnObserverCache::cleanUp, 0, 5, TimeUnit.SECONDS);
  }

  @WithSpan
  private void evaluateTest(Urn entity) {
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "evaluateTestOnChange").time()) {
      log.debug("Evaluating tests for urn {}", entity);
      _testClient.evaluate(entity, null, true, _systemAuthentication);
    } catch (RemoteInvocationException e) {
      MetricUtils.counter(this.getClass(), "evaluteTestOnChangeFailed").inc();
      log.error("Error while evaluating test for entity {}", entity, e);
    }
  }

  @Override
  public void invoke(@NotNull MetadataChangeLog event) throws Exception {
    // Only trigger tests if the change is an UPSERT, change is not for test entity
    if (event.getChangeType() != ChangeType.UPSERT || event.getEntityType().equals(Constants.TEST_ENTITY_NAME)) {
      return;
    }
    // Do not trigger tests if the change is for the TestResults aspect (i.e. result of this hook)
    if (event.getAspectName() != null && event.getAspectName().equals(Constants.TEST_RESULTS_ASPECT_NAME)) {
      return;
    }

    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      return;
    }
    // Put the urn in the observer cache, signifying that the entity for the given urn has been updated
    // This will eventually run the process that evaluates the tests
    // We take this approach to make sure tests are not run for a given urn multiple times when a batch of ingestion events come in
    _urnObserverCache.put(EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec()),
        System.currentTimeMillis());
  }
}
