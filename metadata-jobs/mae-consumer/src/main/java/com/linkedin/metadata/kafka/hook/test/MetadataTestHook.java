package com.linkedin.metadata.kafka.hook.test;

import static com.linkedin.metadata.Constants.*;

import com.codahale.metrics.Timer;
import com.datahub.authentication.Authentication;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.client.MetadataTestClientFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.test.util.TestUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.BatchedTestResults;
import com.linkedin.test.MetadataTestClient;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * This hook serves two functions.
 *
 * <p>1. On Change of a Metadata Test Definition - It can evaluate the test.
 *
 * <p>2. On Change of any entity - It can evaluate any tests associated with that entity for that
 * entity
 *
 * <p>Note, it uses a cache to make sure we run tests once even if multiple update events for the
 * given entity comes in Each of these functions can be independently turned on or off.
 */
@Slf4j
@Component
@Import({
  MetadataTestClientFactory.class,
  SystemAuthenticationFactory.class,
})
public class MetadataTestHook implements MetadataChangeLogHook {

  private final EntityRegistry _entityRegistry;
  private final MetadataTestClient _testClient;
  private final Authentication _systemAuthentication;
  private final Cache<Urn, Long> _urnObserverCache;
  private final Set<String> _supportedEntityTypes;
  private final boolean _isEnabled;

  // Set of aspects to ignore a.k.a do not run tests when these aspects change
  // TestResults needs to be ignored, because otherwise tests will always trigger twice
  // (once when aspect changes -> once when test results changes when the test is evaluated)
  // Status is ignored for now, as massive delete operations can cause massive test triggering
  private static final Set<String> ASPECTS_TO_IGNORE =
      ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME, Constants.STATUS_ASPECT_NAME);
  private final Boolean _isOnTestChangeEnabled;
  private final Boolean _isOnEntityChangeEnabled;

  @Autowired
  public MetadataTestHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull @Value("${metadataTests.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${metadataTests.hook.onTestChange:true}") Boolean onTestChangeEnabled,
      @Nonnull @Value("${metadataTests.hook.onEntityChange:false}") Boolean onEntityChangeEnabled) {
    this(
        entityRegistry,
        testClient,
        systemAuthentication,
        isEnabled,
        2,
        TimeUnit.SECONDS,
        onTestChangeEnabled,
        onEntityChangeEnabled);
  }

  @VisibleForTesting
  MetadataTestHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull @Value("${metadataTests.hook.enabled:true}") Boolean isEnabled,
      final int cacheExpirationTime,
      final TimeUnit cacheExpirationUnit) {
    // Defaults here indicate the previous behavior where enabled true implied that we are
    // processing entity changes
    this(
        entityRegistry,
        testClient,
        systemAuthentication,
        isEnabled,
        cacheExpirationTime,
        cacheExpirationUnit,
        false,
        isEnabled);
  }

  @VisibleForTesting
  MetadataTestHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull @Value("${metadataTests.hook.enabled:true}") Boolean isEnabled,
      final int cacheExpirationTime,
      final TimeUnit cacheExpirationUnit,
      @Nonnull @Value("${metadataTests.hook.onTestChange:true}") Boolean onTestChangeEnabled,
      @Nonnull @Value("${metadataTests.hook.onEntityChange:false}") Boolean onEntityChangeEnabled) {
    _entityRegistry = entityRegistry;
    _testClient = testClient;
    _systemAuthentication = systemAuthentication;
    _isEnabled = isEnabled;
    _isOnTestChangeEnabled = onTestChangeEnabled;
    _isOnEntityChangeEnabled = onEntityChangeEnabled;
    // Inserts tests to run into an in-memory cache that evaluates tests when the cache entry
    // expires
    _urnObserverCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheExpirationTime, cacheExpirationUnit)
            .removalListener(
                RemovalListeners.asynchronous(
                    (RemovalListener<Urn, Long>)
                        removalNotification -> {
                          if (removalNotification.getCause() == RemovalCause.EXPIRED) {
                            evaluateTest(removalNotification.getKey());
                          }
                        },
                    Executors.newCachedThreadPool()))
            .build();
    _supportedEntityTypes = TestUtils.getSupportedEntityTypes(entityRegistry);
    ScheduledExecutorService cleanUpService = Executors.newScheduledThreadPool(1);
    cleanUpService.scheduleAtFixedRate(_urnObserverCache::cleanUp, 0, 5, TimeUnit.SECONDS);
  }

  @Override
  public boolean isEnabled() {
    return _isEnabled;
  }

  @WithSpan
  private void evaluateTest(Urn entity) {
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "evaluateTestOnChange").time()) {
      log.debug("Evaluating tests for urn {}", entity);
      _testClient.evaluate(entity, null, true, _systemAuthentication);
      MetricUtils.counter(this.getClass(), "evaluateTestOnChangeSucceeded").inc();
    } catch (RemoteInvocationException e) {
      MetricUtils.counter(this.getClass(), "evaluateTestOnChangeFailed").inc();
      log.error("Error while evaluating test for entity {}", entity, e);
    }
  }

  private void processTestChange(MetadataChangeLog event) {
    try {
      BatchedTestResults results =
          _testClient.evaluateSingleTest(
              Objects.requireNonNull(event.getEntityUrn()), true, _systemAuthentication);
      log.info(
          "Test {} evaluated successfully with results for {} entries",
          event.getEntityUrn(),
          results.getResults().keySet().size());
    } catch (RemoteInvocationException e) {
      log.error("Error while evaluating test {}", event.getEntityUrn(), e);
    }
  }

  @Override
  public void invoke(@NotNull MetadataChangeLog event) throws Exception {
    // Check if this is a new or updated Metadata Test
    if (this._isOnTestChangeEnabled
        && event.getChangeType() == ChangeType.UPSERT
        && event.getEntityType().equals(TEST_ENTITY_NAME)
        && event.getAspectName().equals(TEST_INFO_ASPECT_NAME)) {
      log.info(
          "Upsert event for Metadata Test entity {} received, evaluating test",
          event.getEntityUrn());
      processTestChange(event);
      return;
    }

    // Rest of the function deals with Entity changes, so we short-circuit here
    if (!this._isOnEntityChangeEnabled) {
      return;
    }

    // Process other changes in other entities
    // Only trigger tests if the change is an UPSERT, change is not for test entity
    if (event.getChangeType() != ChangeType.UPSERT
        || !_supportedEntityTypes.contains(event.getEntityType())) {
      return;
    }
    // Do not trigger tests if the change is for an aspect among the aspects to ignore set
    if (event.getAspectName() != null && ASPECTS_TO_IGNORE.contains(event.getAspectName())) {
      return;
    }
    // Do not trigger tests if the change comes from an ingestion
    if (event.hasSystemMetadata()
        && event.getSystemMetadata().hasRunId()
        && !event.getSystemMetadata().getRunId().equals(DEFAULT_RUN_ID)) {
      return;
    }

    // Do not trigger if event originated from a Test execution
    if (event.getSystemMetadata() != null) {
      if (event.getSystemMetadata().getProperties() != null) {
        if (METADATA_TESTS_SOURCE.equals(
            event.getSystemMetadata().getProperties().get(APP_SOURCE))) {
          // If coming from the UI, we pre-process the Update Indices hook as a fast path to avoid
          // Kafka lag
          return;
        }
      }
    }

    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      return;
    }

    // Put the urn in the observer cache, signifying that the entity for the given urn has been
    // updated
    // This will eventually run the process that evaluates the tests
    // We take this approach to make sure tests are not run for a given urn multiple times when a
    // batch of ingestion events come in
    _urnObserverCache.put(
        EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec()),
        System.currentTimeMillis());
  }

  @VisibleForTesting
  void cleanUpCache() {
    _urnObserverCache.cleanUp();
  }
}
