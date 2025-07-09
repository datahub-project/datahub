package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.service.util.MonitorServiceUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionMonitorSettings;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Slf4j
public class MonitorService extends BaseService {

  private static final int DEFAULT_RETRY_INTERVAL = 2;
  private static final MonitorType DEFAULT_MONITOR_TYPE = MonitorType.ASSERTION;
  private static final String TEST_ASSERTION_ENDPOINT = "assertions/evaluate_assertion";
  private static final String RUN_ASSERTIONS_ENDPOINT = "assertions/evaluate_assertion_urns";
  private static final String TRAIN_ASSERTION_MONITOR_ENDPOINT =
      "assertions/train_assertion_monitor";

  private final String monitorServiceHost;
  private final Integer monitorServicePort;
  private final String protocol;
  private final CloseableHttpClient httpClient;
  private final BackoffPolicy backoffPolicy;
  private final int retryCount;
  private final OperationContext systemOperationContext;

  public MonitorService(
      @Nonnull final String monitorServiceHost,
      @Nonnull final Integer monitorServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull OperationContext systemOperationContext) {
    this(
        monitorServiceHost,
        monitorServicePort,
        useSsl,
        entityClient,
        HttpClients.createDefault(),
        new ExponentialBackoff(DEFAULT_RETRY_INTERVAL),
        3,
        openApiClient,
        systemOperationContext);
  }

  public MonitorService(
      @Nonnull final String monitorServiceHost,
      @Nonnull final Integer monitorServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final CloseableHttpClient httpClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      final int retryCount,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull OperationContext systemOperationContext) {

    super(entityClient, openApiClient, systemOperationContext.getObjectMapper());

    this.monitorServiceHost = Objects.requireNonNull(monitorServiceHost);
    this.monitorServicePort = Objects.requireNonNull(monitorServicePort);
    this.httpClient = Objects.requireNonNull(httpClient);
    this.protocol = useSsl ? "https" : "http";
    this.backoffPolicy = backoffPolicy;
    this.retryCount = retryCount;
    this.systemOperationContext = systemOperationContext;
  }

  /**
   * Returns an instance of {@link MonitorInfo} for the specified Monitor urn, or null if one cannot
   * be found.
   *
   * @param monitorUrn the urn of the Monitor
   * @return an instance of {@link MonitorInfo} for the Monitor, null if it does not exist.
   */
  @Nullable
  public MonitorInfo getMonitorInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn monitorUrn) {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    final EntityResponse response = getMonitorEntityResponse(opContext, monitorUrn);
    if (response != null && response.getAspects().containsKey(Constants.MONITOR_INFO_ASPECT_NAME)) {
      return new MonitorInfo(
          response.getAspects().get(Constants.MONITOR_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn, or null if one cannot
   * be found.
   *
   * @param opContext the authentication to use
   * @param monitorUrn the urn of the View
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  public EntityResponse getMonitorEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn monitorUrn) {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.MONITOR_ENTITY_NAME,
          monitorUrn,
          ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Monitor with urn %s", monitorUrn), e);
    }
  }

  @Nonnull
  public Urn createAssertionMonitor(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final CronSchedule schedule,
      @Nonnull final AssertionEvaluationParameters parameters,
      @Nullable final String executorId)
      throws Exception {
    return createAssertionMonitor(
        opContext, entityUrn, assertionUrn, schedule, parameters, executorId, null);
  }

  /**
   * Creates a new Dataset or DataJob Freshness Monitor for native execution by DataHub. Assumes
   * that the caller has already performed the required authorization.
   *
   * <p>Throws an exception if the provided assertion urn does not exist.
   */
  @Nonnull
  public Urn createAssertionMonitor(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final CronSchedule schedule,
      @Nonnull final AssertionEvaluationParameters parameters,
      @Nullable final String executorId,
      @Nullable final AssertionMonitorSettings monitorSettings)
      throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");

    // Verify that the target entity actually exists.
    validateEntity(opContext, entityUrn);

    // Verify that the target assertion actually exists.
    validateEntity(opContext, assertionUrn);

    final Urn monitorUrn = generateMonitorUrn(entityUrn);

    final MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);

    // New monitors will default to 'Active' mode.
    monitorInfo.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));

    if (executorId != null) {
      monitorInfo.setExecutorId(executorId);
    }

    final AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(
        new AssertionEvaluationSpecArray(
            ImmutableList.of(
                new AssertionEvaluationSpec()
                    .setAssertion(assertionUrn)
                    .setSchedule(schedule)
                    .setParameters(parameters))));
    if (monitorSettings != null) {
      assertionMonitor.setSettings(monitorSettings);
    }
    monitorInfo.setAssertionMonitor(assertionMonitor);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, monitorInfo));

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to create new Monitor for Assertion with urn %s", assertionUrn), e);
    }
  }

  public Urn upsertAssertionMonitor(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn monitorUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final CronSchedule schedule,
      @Nonnull final AssertionEvaluationParameters parameters,
      @Nonnull final MonitorMode mode,
      @Nullable final String executorId)
      throws Exception {
    return upsertAssertionMonitor(
        opContext,
        monitorUrn,
        assertionUrn,
        entityUrn,
        schedule,
        parameters,
        mode,
        executorId,
        null);
  }

  /**
   * Updates an existing Dataset or DataJob Freshness Monitor for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   *
   * <p>Throws an exception if the provided assertion urn does not exist.
   */
  public Urn upsertAssertionMonitor(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn monitorUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final CronSchedule schedule,
      @Nonnull final AssertionEvaluationParameters parameters,
      @Nonnull final MonitorMode mode,
      @Nullable final String executorId,
      @Nullable final AssertionMonitorSettings monitorSettings)
      throws Exception {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");

    // Verify that the target assertion actually exists.
    validateEntity(opContext, assertionUrn);

    // Verify that the target entity actually exists.
    validateEntity(opContext, entityUrn);

    MonitorInfo maybeExistingInfo = getMonitorInfo(opContext, monitorUrn);

    if (maybeExistingInfo != null) {
      AssertionMonitor assertionMonitor = maybeExistingInfo.getAssertionMonitor();
      if (assertionMonitor == null
          || assertionMonitor.getAssertions(GetMode.NULL) == null
          || assertionMonitor.getAssertions().isEmpty()) {
        throw new Exception(
            String.format(
                "Failed to update Assertion Monitor. Monitor with urn %s is not linked to any Assertion.",
                monitorUrn, assertionUrn));
      }
      AssertionEvaluationSpecArray existingAssertions =
          maybeExistingInfo.getAssertionMonitor().getAssertions();
      Urn existingAssertionUrn = existingAssertions.get(0).getAssertion();

      if (!assertionUrn.equals(existingAssertionUrn)) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to update Assertion Monitor. Assertion with urn %s is not linked to Monitor with urn %s.",
                assertionUrn, monitorUrn));
      }
    }

    final MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);

    monitorInfo.setStatus(new MonitorStatus().setMode(mode));

    final AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(
        new AssertionEvaluationSpecArray(
            ImmutableList.of(
                new AssertionEvaluationSpec()
                    .setAssertion(assertionUrn)
                    .setSchedule(schedule)
                    .setParameters(parameters))));
    if (monitorSettings != null) {
      assertionMonitor.setSettings(monitorSettings);
    }
    monitorInfo.setAssertionMonitor(assertionMonitor);
    monitorInfo.setExecutorId(executorId, SetMode.IGNORE_NULL);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, monitorInfo));

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      performPostUpsertActions(monitorUrn, maybeExistingInfo, monitorSettings);
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert Monitor with urn %s", monitorUrn), e);
    }
  }

  /**
   * Upserts the mode of a particular monitor, to enable or disable it's operation.
   *
   * <p>If the monitor does not yet have an info aspect, a default will be minted for the provided
   * urn.
   */
  @Nonnull
  public Urn upsertMonitorMode(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn monitorUrn,
      @Nonnull final MonitorMode monitorMode)
      throws Exception {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(monitorMode, "monitorMode must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");

    MonitorInfo info = getMonitorInfo(opContext, monitorUrn);

    // If monitor info does not yet exist, then mint a default info aspect
    if (info == null) {
      info =
          new MonitorInfo()
              .setType(DEFAULT_MONITOR_TYPE)
              .setAssertionMonitor(
                  new AssertionMonitor()
                      .setAssertions(new AssertionEvaluationSpecArray(Collections.emptyList())));
    }

    // Update the status to have the new Monitor Mode
    info.setStatus(new MonitorStatus().setMode(monitorMode));

    // Write the info back!
    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, info));

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert Monitor Mode for monitor with urn %s", monitorUrn), e);
    }
  }

  @Nonnull
  public AssertionResult testFreshnessAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final Urn connectionUrn,
      @Nonnull final FreshnessAssertionInfo freshnessAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters) {
    Objects.requireNonNull(asserteeUrn, "asserteeUrn must not be null");
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    Objects.requireNonNull(freshnessAssertionInfo, "freshnessAssertionInfo must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");

    try {
      final String jsonBody =
          MonitorServiceUtils.buildTestFreshnessAssertionBodyJson(
              "FRESHNESS",
              asserteeUrn.toString(),
              connectionUrn.toString(),
              freshnessAssertionInfo,
              parameters);
      return testAssertion(jsonBody);
    } catch (Exception e) {
      log.error("Failed to test Freshness assertion.", e);
      return null;
    }
  }

  @Nonnull
  public AssertionResult testVolumeAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final Urn connectionUrn,
      @Nonnull final VolumeAssertionInfo volumeAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters) {
    Objects.requireNonNull(asserteeUrn, "asserteeUrn must not be null");
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    Objects.requireNonNull(volumeAssertionInfo, "volumeAssertionInfo must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");

    try {
      final String jsonBody =
          MonitorServiceUtils.buildTestVolumeAssertionBodyJson(
              "VOLUME",
              asserteeUrn.toString(),
              connectionUrn.toString(),
              volumeAssertionInfo,
              parameters);
      return testAssertion(jsonBody);
    } catch (Exception e) {
      log.error("Failed to test Volume assertion.", e);
      return null;
    }
  }

  @Nonnull
  public AssertionResult testSqlAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final Urn connectionUrn,
      @Nonnull final SqlAssertionInfo sqlAssertionInfo) {
    Objects.requireNonNull(asserteeUrn, "asserteeUrn must not be null");
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    Objects.requireNonNull(sqlAssertionInfo, "sqlAssertionInfo must not be null");

    try {
      final String jsonBody =
          MonitorServiceUtils.buildTestSqlAssertionBodyJson(
              "SQL", asserteeUrn.toString(), connectionUrn.toString(), sqlAssertionInfo);
      return testAssertion(jsonBody);
    } catch (Exception e) {
      log.error("Failed to test SQL assertion.", e);
      return null;
    }
  }

  @Nonnull
  public AssertionResult testFieldAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final Urn connectionUrn,
      @Nonnull final FieldAssertionInfo fieldAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters) {
    Objects.requireNonNull(asserteeUrn, "asserteeUrn must not be null");
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    Objects.requireNonNull(fieldAssertionInfo, "fieldAssertionInfo must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");

    try {
      final String jsonBody =
          MonitorServiceUtils.buildTestFieldAssertionBodyJson(
              "FIELD",
              asserteeUrn.toString(),
              connectionUrn.toString(),
              fieldAssertionInfo,
              parameters);
      return testAssertion(jsonBody);
    } catch (Exception e) {
      log.error("Failed to test FIELD assertion.", e);
      return null;
    }
  }

  @Nonnull
  public AssertionResult testSchemaAssertion(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final Urn connectionUrn,
      @Nonnull final SchemaAssertionInfo schemaAssertionInfo,
      @Nonnull final AssertionEvaluationParameters parameters) {
    Objects.requireNonNull(asserteeUrn, "asserteeUrn must not be null");
    Objects.requireNonNull(connectionUrn, "connectionUrn must not be null");
    Objects.requireNonNull(schemaAssertionInfo, "schemaAssertionInfo must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");

    try {
      final String jsonBody =
          MonitorServiceUtils.buildTestSchemaAssertionBodyJson(
              "DATA_SCHEMA",
              asserteeUrn.toString(),
              connectionUrn.toString(),
              schemaAssertionInfo,
              parameters);
      return testAssertion(jsonBody);
    } catch (Exception e) {
      log.error("Failed to test DATA_SCHEMA assertion.", e);
      return null;
    }
  }

  /**
   * Runs the specified assertions on demand by invoking an executor service endpoint.
   *
   * @param assertionUrns the list of assertion urns to run
   * @param dryRun whether to run the assertions in dry run mode
   * @param parameters the parameters to pass to the assertions
   * @param async whether to run the assertions asynchronously
   * @return a map of assertion urns to their corresponding assertion results
   */
  public Map<Urn, AssertionResult> runAssertions(
      @Nonnull final List<Urn> assertionUrns,
      final boolean dryRun,
      final @Nonnull Map<String, String> parameters,
      final boolean async) {
    try {
      String jsonBody =
          MonitorServiceUtils.buildRunAssertionsBodyJson(assertionUrns, dryRun, parameters, async);
      return executeRunAssertions(assertionUrns, jsonBody);
    } catch (Exception e) {
      throw new RuntimeException("Received exception while attempting to run assertions!", e);
    }
  }

  /**
   * Retrains the monitor with the specified urn on demand by invoking an executor service endpoint.
   *
   * @param monitorUrn the urn of the monitor to retrain
   */
  public void retrainAssertionMonitor(@Nonnull final Urn monitorUrn) {
    try {
      String jsonBody = MonitorServiceUtils.buildRetrainMonitorBodyJson(monitorUrn);
      executeRetrainAssertionMonitor(monitorUrn, jsonBody);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while attempting to train assertion monitor! This may mean that the monitor was not retrained successfully.",
          e);
    }
  }

  public void validateEntity(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn)
      throws Exception {
    if (!this.entityClient.exists(opContext, entityUrn)) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to edit Monitor. %s with urn %s does not exist.",
              entityUrn.getEntityType(), entityUrn));
    }
  }

  @Nonnull
  public Urn generateMonitorUrn(@Nonnull final Urn entityUrn) {
    final MonitorKey key = new MonitorKey();
    final String id = UUID.randomUUID().toString();
    key.setEntity(entityUrn);
    key.setId(id);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.MONITOR_ENTITY_NAME);
  }

  private void performPostUpsertActions(
      @Nonnull Urn monitorUrn,
      @Nullable final MonitorInfo maybeExistingInfo,
      @Nullable AssertionMonitorSettings monitorSettings) {
    // If there is an existing monitor, but the inference settings have changed, we need to retrain
    // the monitor on demand.
    if (shouldRetrainMonitor(maybeExistingInfo, monitorSettings)) {
      log.info(
          String.format(
              "Inference settings have changed for monitor with urn %s. Making a call to kick off retraining.",
              monitorUrn));
      // To do this we will execute a request to the integrations service.
      // This will trigger the monitor to retrain on the next scheduled run.
      // We will not wait for the retraining to complete.
      try {
        // Execute retraining async
        CompletableFuture.runAsync(
            () -> {
              try {
                retrainAssertionMonitor(monitorUrn);
              } catch (Exception e) {
                log.error(
                    String.format(
                        "Failed to retrain monitor with urn %s. This may indicate that the monitor was not successfully retrained!",
                        monitorUrn),
                    e);
              }
            });
      } catch (Exception e) {
        log.error(
            String.format(
                "Failed to retrain monitor with urn %s. This may indicate that the monitor was not successfully retrained!",
                monitorUrn),
            e);
      }
    }
  }

  private boolean shouldRetrainMonitor(
      @Nullable final MonitorInfo maybeExistingInfo,
      @Nullable AssertionMonitorSettings newMonitorSettings) {

    if (maybeExistingInfo == null
        || newMonitorSettings == null
        || !newMonitorSettings.hasAdjustmentSettings()) {
      return false; // No existing monitor or no valid new settings → No retrain
    }

    AssertionMonitor existingMonitor = maybeExistingInfo.getAssertionMonitor();
    if (existingMonitor == null) {
      return false; // No existing assertion monitor → No retrain
    }

    AssertionMonitorSettings existingSettings = existingMonitor.getSettings();
    return existingSettings == null
        || !existingSettings.hasAdjustmentSettings()
        || !existingSettings
            .getAdjustmentSettings()
            .equals(newMonitorSettings.getAdjustmentSettings());
  }

  private CloseableHttpResponse executeRequest(@Nonnull final HttpUriRequest request)
      throws Exception {
    int attemptCount = 0;
    while (attemptCount < this.retryCount) {
      try {
        return httpClient.execute(request);
      } catch (Exception ex) {
        systemOperationContext
            .getMetricUtils()
            .ifPresent(
                metricUtils ->
                    metricUtils.increment(
                        MonitorService.class,
                        "exception"
                            + MetricUtils.DELIMITER
                            + ex.getClass().getSimpleName().toLowerCase(),
                        1));
        if (attemptCount == this.retryCount - 1) {
          throw ex;
        } else {
          attemptCount = attemptCount + 1;
          Thread.sleep(this.backoffPolicy.nextBackoff(attemptCount, ex) * 1000);
        }
      }
    }
    // Should never hit this line.
    throw new RuntimeException("Failed to execute request to integrations service!");
  }

  private void addRequestHeaders(@Nonnull final HttpUriRequest request) {
    // Add authorization header with DataHub frontend system id and secret.
    // request.addHeader("Authorization", this.systemAuthentication.getCredentials());
    request.addHeader("Content-Type", "application/json");
  }

  private void executeRetrainAssertionMonitor(
      @Nonnull final Urn monitorUrn, final String jsonBody) {
    try {
      executeRequest(TRAIN_ASSERTION_MONITOR_ENDPOINT, jsonBody);
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrain monitor with urn " + monitorUrn, e);
    }
  }

  @Nullable
  private Map<Urn, AssertionResult> executeRunAssertions(
      @Nonnull List<Urn> assertionUrns, @Nonnull final String jsonBody) {
    try {
      String maybeJsonResponse = executeRequest(RUN_ASSERTIONS_ENDPOINT, jsonBody);
      if (maybeJsonResponse != null) {
        return MonitorServiceUtils.buildRunAssertionsResult(assertionUrns, maybeJsonResponse);
      }
      log.warn("Received empty body response from Monitors Service!");
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Failed to run assertions against the Monitors Service.", e);
    }
  }

  @Nullable
  private AssertionResult testAssertion(@Nonnull final String jsonBody) {
    try {
      String maybeResponseJson = executeRequest(TEST_ASSERTION_ENDPOINT, jsonBody);
      if (maybeResponseJson != null) {
        return MonitorServiceUtils.buildTestAssertionResult(maybeResponseJson);
      }
      log.warn("Received EMPTY response body when testing assertion!");
    } catch (Exception e) {
      log.error("Failed to test assertion.", e);
    }
    return null;
  }

  @Nullable
  private String executeRequest(final String endpoint, final String jsonBody) {
    CloseableHttpResponse response = null;
    try {
      // Build request
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s/%s",
                  protocol, this.monitorServiceHost, this.monitorServicePort, endpoint));

      addRequestHeaders(request);
      request.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

      // Execute request
      response = executeRequest(request);
      final HttpEntity entity = response.getEntity();

      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        return entity != null ? EntityUtils.toString(entity) : null;
      }
      // Received bad response from Monitors Service!
      throw new RuntimeException(
          String.format(
              "Failed to run query against the Monitors Service. Bad response received from the service. %s",
              response.getStatusLine().toString()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute request to monitors service", e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response to monitors service.", e);
      }
    }
  }
}
