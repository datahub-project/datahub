package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
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
import com.linkedin.data.template.StringMap;


@Slf4j
public class MonitorService extends BaseService {

  private static final int DEFAULT_RETRY_INTERVAL = 2;
  private static final MonitorType DEFAULT_MONITOR_TYPE = MonitorType.ASSERTION;
  private static final String TEST_ASSERTION_ENDPOINT = "assertions/evaluate_assertion";

  private final String monitorServiceHost;
  private final Integer monitorServicePort;
  private final Authentication systemAuthentication;
  private final String protocol;
  private final CloseableHttpClient httpClient;
  private final BackoffPolicy backoffPolicy;
  private final int retryCount;

  public MonitorService(
      @Nonnull final String monitorServiceHost,
      @Nonnull final Integer monitorServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final EntityClient entityClient,
      @Nonnull final Authentication systemAuthentication) {
    this(
        monitorServiceHost,
        monitorServicePort,
        useSsl,
        entityClient,
        systemAuthentication,
        HttpClients.createDefault(),
        new ExponentialBackoff(DEFAULT_RETRY_INTERVAL),
        3
    );
  }

  public MonitorService(
    @Nonnull final String monitorServiceHost,
    @Nonnull final Integer monitorServicePort,
    @Nonnull final Boolean useSsl,
    @Nonnull final EntityClient entityClient,
    @Nonnull final Authentication systemAuthentication,
    @Nonnull final CloseableHttpClient httpClient,
    @Nonnull final BackoffPolicy backoffPolicy,
    final int retryCount) {

    super(entityClient, systemAuthentication);

    this.monitorServiceHost = Objects.requireNonNull(monitorServiceHost);
    this.monitorServicePort = Objects.requireNonNull(monitorServicePort);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
    this.httpClient = Objects.requireNonNull(httpClient);
    this.protocol = useSsl ? "https" : "http";
    this.backoffPolicy = backoffPolicy;
    this.retryCount = retryCount;
  }

  /**
   * Returns an instance of {@link MonitorInfo} for the specified Monitor urn,
   * or null if one cannot be found.
   *
   * @param monitorUrn the urn of the Monitor
   *
   * @return an instance of {@link MonitorInfo} for the Monitor, null if it does not exist.
   */
  @Nullable
  public MonitorInfo getMonitorInfo(@Nonnull final Urn monitorUrn) {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    final EntityResponse response = getMonitorEntityResponse(monitorUrn, this.systemAuthentication);
    if (response != null && response.getAspects().containsKey(Constants.MONITOR_INFO_ASPECT_NAME)) {
      return new MonitorInfo(response.getAspects().get(Constants.MONITOR_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn,
   * or null if one cannot be found.
   *
   * @param monitorUrn the urn of the View
   * @param authentication the authentication to use
   *
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  public EntityResponse getMonitorEntityResponse(@Nonnull final Urn monitorUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return this.entityClient.getV2(
          Constants.MONITOR_ENTITY_NAME,
          monitorUrn,
          ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME),
          authentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Monitor with urn %s", monitorUrn), e);
    }
  }

  /**
   * Creates a new Dataset or DataJob Freshness Monitor for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   *
   * Throws an exception if the provided assertion urn does not exist.
   */
  @Nonnull
  public Urn createAssertionMonitor(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final CronSchedule schedule,
      @Nonnull final AssertionEvaluationParameters parameters,
      @Nullable final String executorId,
      @Nonnull final Authentication authentication) throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // Verify that the target entity actually exists.
    validateEntity(entityUrn, authentication);

    // Verify that the target assertion actually exists.
    validateEntity(assertionUrn, authentication);

    final Urn monitorUrn = generateMonitorUrn(entityUrn);

    final MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);

    // New monitors will default to 'Active' mode.
    monitorInfo.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));

    if (executorId != null) {
      monitorInfo.setExecutorId(executorId);
    }

    final AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray(
        ImmutableList.of(
            new AssertionEvaluationSpec()
                .setAssertion(assertionUrn)
                .setSchedule(schedule)
                .setParameters(parameters)
        )
    ));
    monitorInfo.setAssertionMonitor(assertionMonitor);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, monitorInfo));

    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create new Monitor for Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Upserts the mode of a particular monitor, to enable or disable it's operation.
   *
   * If the monitor does not yet have an info aspect, a default will be minted for the provided urn.
   */
  @Nonnull
  public Urn upsertMonitorMode(
      @Nonnull final Urn monitorUrn,
      @Nonnull final MonitorMode monitorMode,
      @Nonnull final Authentication authentication) throws Exception {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(monitorMode, "monitorMode must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    MonitorInfo info = getMonitorInfo(monitorUrn);

    // If monitor info does not yet exist, then mint a default info aspect
    if (info == null) {
      info = new MonitorInfo()
          .setType(DEFAULT_MONITOR_TYPE)
          .setAssertionMonitor(
              new AssertionMonitor().setAssertions(new AssertionEvaluationSpecArray(Collections.emptyList())));
    }

    // Update the status to have the new Monitor Mode
    info.setStatus(new MonitorStatus().setMode(monitorMode));

    // Write the info back!
    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, info));

    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to upsert Monitor Mode for monitor with urn %s", monitorUrn), e);
    }
  }

  private void validateEntity(@Nonnull final Urn entityUrn, @Nonnull final Authentication authentication) throws Exception {
    if (!this.entityClient.exists(entityUrn, authentication)) {
      throw new IllegalArgumentException(String.format("Failed to edit Monitor. %s with urn %s does not exist.", entityUrn
          .getEntityType(), entityUrn));
    }
  }

  @Nonnull
  private Urn generateMonitorUrn(@Nonnull final Urn entityUrn) {
    final MonitorKey key = new MonitorKey();
    final String id = UUID.randomUUID().toString();
    key.setEntity(entityUrn);
    key.setId(id);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.MONITOR_ENTITY_NAME);
  }

  private static String buildTestSqlAssertionBodyJson(
      @Nonnull final String type,
      @Nonnull final String asserteeUrn,
      @Nonnull final String connectionUrn,
      @Nonnull final SqlAssertionInfo sqlAssertionInfo
  ) throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    final ObjectNode assertionNode = objectMapper.createObjectNode();
    final ObjectNode sqlAssertionNode = objectMapper.createObjectNode();
    final ObjectNode sqlParamNode = objectMapper.createObjectNode();
    final ObjectNode paramNode = objectMapper.createObjectNode();

    final AssertionStdParameters parameters = sqlAssertionInfo.getParameters();
    if (parameters.hasValue()) {
      final AssertionStdParameter value = parameters.getValue();
      final ObjectNode valueNode = objectMapper.createObjectNode();
      valueNode.put("type", value.getType().toString());
      valueNode.put("value", value.getValue());
      sqlParamNode.put("value", valueNode);
    }
    if (parameters.hasMaxValue()) {
      final AssertionStdParameter maxValue = parameters.getMaxValue();
      final ObjectNode maxValueNode = objectMapper.createObjectNode();
      maxValueNode.put("type", maxValue.getType().toString());
      maxValueNode.put("value", maxValue.getValue());
      sqlParamNode.put("maxValue", maxValueNode);
    }
    if (parameters.hasMinValue()) {
      final AssertionStdParameter minValue = parameters.getMinValue();
      final ObjectNode minValueNode = objectMapper.createObjectNode();
      minValueNode.put("type", minValue.getType().toString());
      minValueNode.put("value", minValue.getValue());
      sqlParamNode.put("minValue", minValueNode);
    }

    sqlAssertionNode.put("type", sqlAssertionInfo.getType().toString());
    sqlAssertionNode.put("statement", sqlAssertionInfo.getStatement());
    if (sqlAssertionInfo.hasChangeType()) {
      sqlAssertionNode.put("changeType", sqlAssertionInfo.getChangeType().toString());
    }
    sqlAssertionNode.put("operator", sqlAssertionInfo.getOperator().toString());
    sqlAssertionNode.put("parameters", sqlParamNode);
    assertionNode.put("sqlAssertion", sqlAssertionNode);

    paramNode.put("type", "DATASET_SQL");

    objectNode.put("type", type);
    objectNode.put("connectionUrn", connectionUrn);
    objectNode.put("entityUrn", asserteeUrn);
    objectNode.put("assertion", assertionNode);
    objectNode.put("parameters", paramNode);

    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  private static AssertionResult buildTestAssertionResult(@Nonnull final String jsonStr) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      ObjectNode json = (ObjectNode) mapper.readTree(jsonStr);
      final AssertionResult assertionResult = new AssertionResult();

      assertionResult.setType(AssertionResultType.valueOf(json.get("type").asText()));
      if (json.has("rowCount") && !json.get("rowCount").isNull()) {
        assertionResult.setRowCount(json.get("rowCount").asLong());
      }
      if (json.has("missingCount") && !json.get("missingCount").isNull()) {
        assertionResult.setMissingCount(json.get("missingCount").asLong());
      }
      if (json.has("unexpectedCount") && !json.get("unexpectedCount").isNull()) {
        assertionResult.setUnexpectedCount(json.get("unexpectedCount").asLong());
      }
      if (json.has("actualAggValue") && !json.get("actualAggValue").isNull()) {
        assertionResult.setActualAggValue(Float.valueOf(json.get("actualAggValue").asText()));
      }
      if (json.has("externalUrl") && !json.get("externalUrl").isNull()) {
        assertionResult.setExternalUrl(json.get("externalUrl").asText());
      }
      if (json.has("nativeResults") && !json.get("nativeResults").isNull()) {
        Map<String, String> properties = mapper.convertValue(json.get("nativeResults"), new TypeReference<Map<String, String>>() { });
        assertionResult.setNativeResults(new StringMap(properties));
      }
      if (json.has("error") && !json.get("error").isNull()) {
        final AssertionResultError assertionResultError = new AssertionResultError();
        assertionResultError.setType(AssertionResultErrorType.valueOf(json.get("error").get("type").asText()));
        if (json.get("error").has("properties") && !json.get("error").get("properties").isNull()) {
          Map<String, String> properties = mapper.convertValue(json.get("error").get("properties"), new TypeReference<Map<String, String>>() { });
          assertionResultError.setProperties(new StringMap(properties));
        }
        assertionResult.setError(assertionResultError);
      }

      return assertionResult;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON received from the Monitors service! %s");
    }
  }

  private CloseableHttpResponse executeRequest(@Nonnull final HttpUriRequest request) throws Exception {
    int attemptCount = 0;
    while (attemptCount < this.retryCount) {
      try {
        return httpClient.execute(request);
      } catch (Exception ex) {
        MetricUtils.counter(MonitorService.class, "exception" + MetricUtils.DELIMITER + ex.getClass().getName().toLowerCase()).inc();
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

  private AssertionResult testAssertion(
    @Nonnull final String jsonBody
  ) {
    CloseableHttpResponse response = null;
    try {
      // Build request
      final HttpPost request = new HttpPost(String.format("%s://%s:%s/%s",
          protocol,
          this.monitorServiceHost,
          this.monitorServicePort,
          TEST_ASSERTION_ENDPOINT));

      addRequestHeaders(request);

      request.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

      // Execute request
      response = executeRequest(request);
      final HttpEntity entity = response.getEntity();

      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        final String jsonStr = EntityUtils.toString(entity);
        return buildTestAssertionResult(jsonStr);
      }
      // Otherwise, something went wrong!
      log.error(
          String.format("Bad response from the Monitors Service: %s", response.getStatusLine().toString()));
      return null;
    } catch (Exception e) {
      log.error("Failed to test assertion.", e);
      return null;
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response to Montiors service.", e);
      }
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
      final String jsonBody = buildTestSqlAssertionBodyJson(
        "SQL",
        asserteeUrn.toString(),
        connectionUrn.toString(),
        sqlAssertionInfo
      );
      return testAssertion(jsonBody);
    } catch (Exception e) {
      log.error("Failed to test assertion.", e);
      return null;
    }
  }
}