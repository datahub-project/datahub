package com.linkedin.datahub.graphql.resolvers.ingest;

import com.linkedin.common.urn.Urn;
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
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IngestionResolverUtils {

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
    final EnvelopedAspect envelopedInput =
        aspects.get(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    if (envelopedInput != null) {
      final ExecutionRequestInput executionRequestInput =
          new ExecutionRequestInput(envelopedInput.getValue().data());
      final com.linkedin.datahub.graphql.generated.ExecutionRequestInput inputResult =
          new com.linkedin.datahub.graphql.generated.ExecutionRequestInput();

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
      result.setInput(inputResult);
    }

    // Map result aspect. Optional.
    final EnvelopedAspect envelopedResult =
        aspects.get(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
    if (envelopedResult != null) {
      final ExecutionRequestResult executionRequestResult =
          new ExecutionRequestResult(envelopedResult.getValue().data());
      result.setResult(mapExecutionRequestResult(executionRequestResult));
    }

    return result;
  }

  public static com.linkedin.datahub.graphql.generated.ExecutionRequestSource
      mapExecutionRequestSource(final ExecutionRequestSource execRequestSource) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestSource result =
        new com.linkedin.datahub.graphql.generated.ExecutionRequestSource();
    result.setType(execRequestSource.getType());
    return result;
  }

  public static com.linkedin.datahub.graphql.generated.ExecutionRequestResult
      mapExecutionRequestResult(final ExecutionRequestResult execRequestResult) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestResult result =
        new com.linkedin.datahub.graphql.generated.ExecutionRequestResult();
    result.setStatus(execRequestResult.getStatus());
    result.setStartTimeMs(execRequestResult.getStartTimeMs());
    result.setDurationMs(execRequestResult.getDurationMs());
    result.setReport(execRequestResult.getReport());
    if (execRequestResult.hasStructuredReport()) {
      result.setStructuredReport(mapStructuredReport(execRequestResult.getStructuredReport()));
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
    final DataHubIngestionSourceInfo ingestionSourceInfo =
        new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

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
    result.setExecutorId(config.getExecutorId());
    result.setDebugMode(config.isDebugMode());
    if (config.getExtraArgs() != null) {
      List<StringMapEntry> extraArgs =
          config.getExtraArgs().keySet().stream()
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

  private IngestionResolverUtils() {}
}
