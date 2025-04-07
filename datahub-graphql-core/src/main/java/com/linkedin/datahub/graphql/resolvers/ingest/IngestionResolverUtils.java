package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.linkedin.datahub.graphql.AcrylConstants.INGESTION_SOURCE_EXECUTOR_CLI;
import static com.linkedin.metadata.Constants.DEFAULT_EXECUTOR_ID;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.IngestionConfig;
import com.linkedin.datahub.graphql.generated.IngestionSchedule;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.generated.StructuredReport;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.RunInfo;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IngestionResolverUtils {

  private static final String DATABRICKS_INGESTION_SOURCE_TYPE = "databricks";
  private static final String UNITY_CATALOG_INGESTION_SOURCE_TYPE = "unity-catalog";
  private static final String HIVE_PLATFORM_URN = "urn:li:dataPlatform:hive";
  private static final String DATABRICKS_PLATFORM_URN = "urn:li:dataPlatform:databricks";

  public static List<ExecutionRequest> mapExecutionRequests(
      @Nullable QueryContext context, final Collection<EntityResponse> requests) {
    List<ExecutionRequest> result = new ArrayList<>();
    for (final EntityResponse request : requests) {
      result.add(mapExecutionRequest(context, request));
    }
    return result;
  }

  public static ExecutionRequest mapExecutionRequest(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    final ExecutionRequest result = new ExecutionRequest();
    result.setUrn(entityUrn.toString());
    result.setId(entityUrn.getId());

    // Map input aspect. Must be present.
    final EnvelopedAspect envelopedInput = aspects.get(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    if (envelopedInput != null) {
      final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput(envelopedInput.getValue().data());
      final com.linkedin.datahub.graphql.generated.ExecutionRequestInput inputResult = new com.linkedin.datahub.graphql.generated.ExecutionRequestInput();

      inputResult.setTask(executionRequestInput.getTask());
      if (executionRequestInput.hasSource()) {
        inputResult.setSource(mapExecutionRequestSource(executionRequestInput.getSource()));
      }
      if (executionRequestInput.hasArgs()) {
        inputResult.setArguments(StringMapMapper.map(context, executionRequestInput.getArgs()));
      }
      inputResult.setRequestedAt(executionRequestInput.getRequestedAt());
      if (executionRequestInput.getActorUrn() != null) {
        inputResult.setActorUrn(executionRequestInput.getActorUrn().toString());
      }
      if (executionRequestInput.hasExecutorId()) {
        inputResult.setExecutorId(executionRequestInput.getExecutorId());
      }

      result.setInput(inputResult);
    }

    // Map result aspect. Optional.
    final EnvelopedAspect envelopedResult = aspects.get(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
    if (envelopedResult != null) {
      final ExecutionRequestResult executionRequestResult = new ExecutionRequestResult(
          envelopedResult.getValue().data());
      result.setResult(mapExecutionRequestResult(executionRequestResult));
    }

    return result;
  }

  public static com.linkedin.datahub.graphql.generated.ExecutionRequestSource mapExecutionRequestSource(
      final ExecutionRequestSource execRequestSource) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestSource result = new com.linkedin.datahub.graphql.generated.ExecutionRequestSource();
    result.setType(execRequestSource.getType());
    if (execRequestSource.hasIngestionSource()) {
      result.setIngestionSource(execRequestSource.getIngestionSource().toString());
    }
    return result;
  }

  public static com.linkedin.datahub.graphql.generated.ExecutionRequestResult mapExecutionRequestResult(
      final ExecutionRequestResult execRequestResult) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestResult result = new com.linkedin.datahub.graphql.generated.ExecutionRequestResult();
    result.setStatus(execRequestResult.getStatus());
    result.setStartTimeMs(execRequestResult.getStartTimeMs());
    result.setDurationMs(execRequestResult.getDurationMs());
    result.setReport(execRequestResult.getReport());
    if (execRequestResult.hasStructuredReport()) {
      result.setStructuredReport(mapStructuredReport(execRequestResult.getStructuredReport()));
    }
    if (execRequestResult.hasExecutorInstanceId()) { // SaaS only
      result.setExecutorInstanceId(execRequestResult.getExecutorInstanceId());
    }
    return result;
  }

  public static StructuredReport mapStructuredReport(
      final StructuredExecutionReport structuredReport) {
    StructuredReport structuredReportResult = new StructuredReport();
    structuredReportResult.setType(structuredReport.getType());
    structuredReportResult.setSerializedValue(structuredReport.getSerializedValue());
    structuredReportResult.setContentType(structuredReport.getContentType());
    return structuredReportResult;
  }

  public static List<IngestionSource> mapIngestionSources(
      final Collection<EntityResponse> entities) {
    final List<IngestionSource> results = new ArrayList<>();
    for (EntityResponse response : entities) {
      try {
        results.add(mapIngestionSource(response));
      } catch (IllegalStateException e) {
        log.error("Unable to map ingestion source, continuing to other sources.", e);
      }
    }
    return results;
  }

  public static IngestionSource mapIngestionSource(final EntityResponse ingestionSource) {
    final Urn entityUrn = ingestionSource.getUrn();
    final EnvelopedAspectMap aspects = ingestionSource.getAspects();

    // There should ALWAYS be an info aspect.
    final EnvelopedAspect envelopedInfo = aspects.get(Constants.INGESTION_INFO_ASPECT_NAME);

    if (envelopedInfo == null) {
      throw new IllegalStateException(
          "No ingestion source info aspect exists for urn: " + entityUrn);
    }

    // Bind into a strongly typed object.
    final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(
        envelopedInfo.getValue().data());

    return mapIngestionSourceInfo(entityUrn, ingestionSourceInfo);
  }

  public static IngestionSource mapIngestionSourceInfo(
      final Urn urn, final DataHubIngestionSourceInfo info) {
    final IngestionSource result = new IngestionSource();
    result.setUrn(urn.toString());
    result.setName(info.getName());
    result.setType(info.getType());
    result.setConfig(mapIngestionSourceConfig(info.getConfig()));
    if (info.hasSchedule()) {
      result.setSchedule(mapIngestionSourceSchedule(info.getSchedule()));
    }
    return result;
  }

  public static IngestionConfig mapIngestionSourceConfig(
      final DataHubIngestionSourceConfig config) {
    final IngestionConfig result = new IngestionConfig();
    result.setRecipe(config.getRecipe());
    result.setVersion(config.getVersion());
    if (config.hasExecutorId()) {
      result.setExecutorId(config.getExecutorId());
    } else {
      result.setExecutorId(DEFAULT_EXECUTOR_ID);
    }
    result.setDebugMode(config.isDebugMode());
    if (config.getExtraArgs() != null) {
      List<StringMapEntry> extraArgs = config.getExtraArgs().keySet().stream()
          .map(key -> new StringMapEntry(key, config.getExtraArgs().get(key)))
          .collect(Collectors.toList());
      result.setExtraArgs(extraArgs);
    }
    return result;
  }

  public static IngestionSchedule mapIngestionSourceSchedule(
      final DataHubIngestionSourceSchedule schedule) {
    final IngestionSchedule result = new IngestionSchedule();
    result.setInterval(schedule.getInterval());
    result.setTimezone(schedule.getTimezone());
    return result;
  }

  @Nullable
  public static IngestionSource getIngestionSourceForEntity(
      @Nonnull final EntityClient entityClient,
      @Nonnull final QueryContext context,
      @Nonnull final Urn entityUrn,
      @Nullable final Set<String> aspectNames)
      throws Exception {
    // Fetch the aspects for the entity.
    final EntityResponse entityResponse = entityClient.getV2(
        context.getOperationContext(), entityUrn.getEntityType(), entityUrn, aspectNames);

    // Entity does not exist! Return no source.
    if (entityResponse == null) {
      return null;
    }

    // Get the latest "run id" for the entity
    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();

    // https://linear.app/acryl-data/issue/OBS-56/collect-the-last-run-id-in-the-system-metadata-payload
    // this runId may or may not be the correct (last one) - so in some cases this
    // resolver may
    // return false incorrectly.
    final List<RunInfo> runs = SystemMetadataUtils.getLastIngestionRuns(aspectMap);

    if (runs.isEmpty()) {
      return null;
    }

    // For each run, try to link back to an ingestion source that produced it.
    for (RunInfo run : runs) {
      IngestionSource ingestionSource = tryGetIngestionSourceForRunId(entityClient, run.getId(), entityUrn, context);
      if (ingestionSource != null) {
        return ingestionSource;
      }
    }
    return null;
  }

  @Nullable
  private static IngestionSource tryGetIngestionSourceForRunId(
      @Nonnull final EntityClient entityClient,
      @Nonnull final String runId,
      @Nonnull final Urn entityUrn,
      @Nonnull final QueryContext context)
      throws Exception {

    final Urn runUrn = Urn.createFromString(String.format("urn:li:%s:", EXECUTION_REQUEST_ENTITY_NAME) + runId);

    final EntityResponse executionRequestEntityResponse = entityClient.getV2(
        context.getOperationContext(),
        Constants.EXECUTION_REQUEST_ENTITY_NAME,
        runUrn,
        Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME));

    // If no execution request, return null.
    if (executionRequestEntityResponse == null) {
      return null;
    }

    // Pull out the execution request source, which should have the ingestion source
    final EnvelopedAspectMap executionRequestAspects = executionRequestEntityResponse.getAspects();
    if (!executionRequestAspects.containsKey(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME)) {
      return null;
    }

    final EnvelopedAspect executionRequestEnvelopedInput = executionRequestAspects
        .get(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput(
        executionRequestEnvelopedInput.getValue().data());
    final ExecutionRequestSource execRequestSource = executionRequestInput.getSource();

    // Get the ingestionSource entity to check for the source -> config ->
    // executorId.
    final EntityResponse ingestionSourceEntityResponse = entityClient.getV2(
        context.getOperationContext(),
        Constants.INGESTION_SOURCE_ENTITY_NAME,
        execRequestSource.getIngestionSource(),
        Collections.singleton(INGESTION_INFO_ASPECT_NAME));

    // If we cannot find the ingestion source, return null.
    if (ingestionSourceEntityResponse == null) {
      return null;
    }

    final EnvelopedAspectMap ingestionInfoAspects = ingestionSourceEntityResponse.getAspects();
    if (!ingestionInfoAspects.containsKey(Constants.INGESTION_INFO_ASPECT_NAME)) {
      return null;
    }

    final EnvelopedAspect ingestionSourceEnvelopedInfo = ingestionInfoAspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
    final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(
        ingestionSourceEnvelopedInfo.getValue().data());
    final DataHubIngestionSourceConfig ingestionSourceConfig = ingestionSourceInfo.getConfig();

    // executorId's from the CLI OR REMOTE executor are not valid because we don't
    // have auth info to
    // fetch data.
    final String executorId = ingestionSourceConfig.getExecutorId();
    final boolean isCorrectSource = isMatchingDataPlatform(entityUrn, ingestionSourceInfo.getType())
        && !INGESTION_SOURCE_EXECUTOR_CLI.equals(executorId);

    return isCorrectSource
        ? IngestionResolverUtils.mapIngestionSource(ingestionSourceEntityResponse)
        : null;
  }

  private static Boolean isMatchingDataPlatform(
      @Nonnull final Urn entityUrn, @Nonnull final String type) {
    if (Constants.DATASET_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      // it's possible that another ingestion source produced an aspect for the entity
      // in cases of
      // Datasets
      // e.g. for DBT. here we check the data platform urn and compare it to the
      // ingestion source
      // type.
      Urn dataPlatformUrn = UrnUtils.getUrn(entityUrn.getEntityKey().get(0));
      return type.equalsIgnoreCase(dataPlatformUrn.getId()) || isDatabricksEntity(entityUrn, type);
    }
    return true;
  }

  // For databricks entities, the data platform URN will not necessarily match the
  // ingestion source
  // type.
  private static boolean isDatabricksEntity(
      @Nonnull final Urn entityUrn, @Nonnull final String type) {
    return (type.equalsIgnoreCase(DATABRICKS_INGESTION_SOURCE_TYPE)
        || type.equalsIgnoreCase(UNITY_CATALOG_INGESTION_SOURCE_TYPE))
        && (entityUrn.toString().contains(DATABRICKS_PLATFORM_URN)
            || entityUrn.toString().contains(HIVE_PLATFORM_URN));
  }

  private IngestionResolverUtils() {
  }
}
