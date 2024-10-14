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
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.BatchedTestResults;
import com.linkedin.test.MetadataTestClient;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestInterval;
import com.linkedin.test.TestSourceType;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import lombok.Getter;
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

  private final EntityRegistry entityRegistry;
  private final MetadataTestClient testClient;
  private final Authentication systemAuthentication;
  private final Cache<Urn, Long> urnObserverCache;
  private final Set<String> supportedEntityTypes;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  // Set of aspects to ignore a.k.a do not run tests when these aspects change
  // TestResults needs to be ignored, because otherwise tests will always trigger twice
  // (once when aspect changes -> once when test results changes when the test is evaluated)
  // Status is ignored for now, as massive delete operations can cause massive test triggering
  private static final Set<String> ASPECTS_TO_IGNORE =
      ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME, Constants.STATUS_ASPECT_NAME);
  private final Boolean isOnTestChangeEnabled;
  private final Boolean isOnEntityChangeEnabled;

  @Autowired
  public MetadataTestHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull @Value("${metadataTests.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${metadataTests.hook.onTestChange:true}") Boolean onTestChangeEnabled,
      @Nonnull @Value("${metadataTests.hook.onEntityChange:false}") Boolean onEntityChangeEnabled,
      @Nonnull @Value("${metadataTests.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this(
        entityRegistry,
        testClient,
        systemAuthentication,
        isEnabled,
        2,
        TimeUnit.SECONDS,
        onTestChangeEnabled,
        onEntityChangeEnabled,
        consumerGroupSuffix);
  }

  @VisibleForTesting
  MetadataTestHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull Boolean isEnabled,
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
        isEnabled,
        "");
  }

  MetadataTestHook(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final MetadataTestClient testClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull Boolean isEnabled,
      final int cacheExpirationTime,
      final TimeUnit cacheExpirationUnit,
      @Nonnull Boolean onTestChangeEnabled,
      @Nonnull Boolean onEntityChangeEnabled,
      @Nonnull String consumerGroupSuffix) {
    this.entityRegistry = entityRegistry;
    this.testClient = testClient;
    this.systemAuthentication = systemAuthentication;
    this.isEnabled = isEnabled;
    isOnTestChangeEnabled = onTestChangeEnabled;
    isOnEntityChangeEnabled = onEntityChangeEnabled;
    // Inserts tests to run into an in-memory cache that evaluates tests when the cache entry
    // expires
    urnObserverCache =
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
    supportedEntityTypes = TestUtils.getSupportedEntityTypes(entityRegistry);
    ScheduledExecutorService cleanUpService = Executors.newScheduledThreadPool(1);
    cleanUpService.scheduleAtFixedRate(urnObserverCache::cleanUp, 0, 5, TimeUnit.SECONDS);
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @WithSpan
  private void evaluateTest(Urn entity) {
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "evaluateTestOnChange").time()) {
      log.debug("Evaluating tests for urn {}", entity);
      testClient.evaluate(entity, null, true, systemAuthentication);
      MetricUtils.counter(this.getClass(), "evaluateTestOnChangeSucceeded").inc();
    } catch (RemoteInvocationException e) {
      MetricUtils.counter(this.getClass(), "evaluateTestOnChangeFailed").inc();
      log.error("Error while evaluating test for entity {}", entity, e);
    }
  }

  private void processTestChange(MetadataChangeLog event) {
    try {
      BatchedTestResults results =
          testClient.evaluateSingleTest(
              Objects.requireNonNull(event.getEntityUrn()), true, systemAuthentication);
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
    if (this.isOnTestChangeEnabled
        && event.getChangeType() == ChangeType.UPSERT
        && event.getEntityType().equals(TEST_ENTITY_NAME)
        && event.getAspectName().equals(TEST_INFO_ASPECT_NAME)
        && !isOnDemandTest(event) // don't process on demand tests
        && !isFormPromptTest(event)
        && !isFormsTest(event)) { // don't process form prompt tests on change
      log.info(
          "Upsert event for Metadata Test entity {} received, evaluating test",
          event.getEntityUrn());
      processTestChange(event);
      return;
    }

    // Rest of the function deals with Entity changes, so we short-circuit here
    if (!this.isOnEntityChangeEnabled) {
      return;
    }

    // Process other changes in other entities
    // Only trigger tests if the change is an UPSERT, change is not for test entity
    if (event.getChangeType() != ChangeType.UPSERT
        || !supportedEntityTypes.contains(event.getEntityType())) {
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
      entitySpec = entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      return;
    }

    // Put the urn in the observer cache, signifying that the entity for the given urn has been
    // updated
    // This will eventually run the process that evaluates the tests
    // We take this approach to make sure tests are not run for a given urn multiple times when a
    // batch of ingestion events come in
    urnObserverCache.put(
        EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec()),
        System.currentTimeMillis());
  }

  /*
   * Tests are on-demand if there is explicitly no schedule assigned to them.
   * These tests should not be processed and produce side effects from this hook.
   */
  private boolean isOnDemandTest(@Nonnull MetadataChangeLog event) {
    if (!event.getEntityType().equals(TEST_ENTITY_NAME)
        || !event.getAspectName().equals(TEST_INFO_ASPECT_NAME)) {
      return false;
    }

    TestInfo testInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), TestInfo.class);

    return testInfo.getSchedule() != null
        && testInfo.getSchedule().getInterval().equals(TestInterval.NONE);
  }

  /*
   * If the test source type is FORM_PROMPT we should not be running them on test changes.
   * It creates unnecessary load, as we should rely on entity changes and nightly runs.
   * Prompts should not be changing once the form is published anyways.
   */
  private boolean isFormPromptTest(@Nonnull MetadataChangeLog event) {
    if (!event.getEntityType().equals(TEST_ENTITY_NAME)
        || !event.getAspectName().equals(TEST_INFO_ASPECT_NAME)) {
      return false;
    }

    TestInfo testInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), TestInfo.class);

    return testInfo.getSource() != null
        && testInfo.getSource().getType().equals(TestSourceType.FORM_PROMPT);
  }

  /*
   * If the test source type is FORMS we should not be running them on test changes.
   * It creates unnecessary load, as we should rely on entity changes and nightly runs.
   */
  private boolean isFormsTest(@Nonnull MetadataChangeLog event) {
    if (!event.getEntityType().equals(TEST_ENTITY_NAME)
        || !event.getAspectName().equals(TEST_INFO_ASPECT_NAME)) {
      return false;
    }

    TestInfo testInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), TestInfo.class);

    return testInfo.getSource() != null
        && testInfo.getSource().getType().equals(TestSourceType.FORMS);
  }

  @VisibleForTesting
  void cleanUpCache() {
    urnObserverCache.cleanUp();
  }
}
