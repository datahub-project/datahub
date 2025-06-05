package com.linkedin.datahub.graphql.types.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.StructuredReport;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps {@link EntityResponse} representing an ExecutionRequest entity to a GraphQL {@link
 * ExecutionRequest} object.
 */
@Slf4j
public class ExecutionRequestMapper implements ModelMapper<EntityResponse, ExecutionRequest> {
  public static final ExecutionRequestMapper INSTANCE = new ExecutionRequestMapper();

  /**
   * Maps a {@link EntityResponse} to a GraphQL {@link ExecutionRequest} object.
   *
   * @param context the query context
   * @param entityResponse the entity response to map
   * @return the mapped GraphQL ExecutionRequest object
   */
  public static ExecutionRequest map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  /**
   * Applies the mapping from an {@link EntityResponse} to an {@link ExecutionRequest}.
   *
   * @param context The query context.
   * @param entityResponse The entity response to map.
   * @return The mapped {@link ExecutionRequest}.
   */
  @Override
  public ExecutionRequest apply(
      @Nullable QueryContext context, @Nonnull EntityResponse entityResponse) {
    return mapExecutionRequest(context, entityResponse);
  }

  /**
   * Maps an {@link EntityResponse} to an {@link ExecutionRequest}.
   *
   * @param context The query context.
   * @param entityResponse The entity response to map.
   * @return The mapped {@link ExecutionRequest}.
   */
  private ExecutionRequest mapExecutionRequest(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    final ExecutionRequest result = new ExecutionRequest();
    result.setUrn(entityUrn.toString());
    result.setId(entityUrn.getId());
    result.setType(EntityType.EXECUTION_REQUEST);

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
      if (executionRequestInput.hasExecutorId()) {
        inputResult.setExecutorId(executionRequestInput.getExecutorId());
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

  /**
   * Maps a {@link ExecutionRequestSource} to a GraphQL {@link
   * com.linkedin.datahub.graphql.generated.ExecutionRequestSource} object.
   *
   * @param execRequestSource the ExecutionRequestSource to map
   * @return the mapped GraphQL ExecutionRequestSource object
   */
  private com.linkedin.datahub.graphql.generated.ExecutionRequestSource mapExecutionRequestSource(
      final ExecutionRequestSource execRequestSource) {
    final com.linkedin.datahub.graphql.generated.ExecutionRequestSource result =
        new com.linkedin.datahub.graphql.generated.ExecutionRequestSource();
    result.setType(execRequestSource.getType());
    if (execRequestSource.hasIngestionSource()) {
      result.setIngestionSource(execRequestSource.getIngestionSource().toString());
    }
    return result;
  }

  /**
   * Maps a {@link ExecutionRequestResult} to a GraphQL {@link
   * com.linkedin.datahub.graphql.generated.ExecutionRequestResult} object.
   *
   * @param execRequestResult the ExecutionRequestResult to map
   * @return the mapped GraphQL ExecutionRequestResult object
   */
  private com.linkedin.datahub.graphql.generated.ExecutionRequestResult mapExecutionRequestResult(
      final ExecutionRequestResult execRequestResult) {
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

  /**
   * Maps a {@link StructuredExecutionReport} to a GraphQL {@link StructuredReport} object.
   *
   * @param structuredReport the StructuredExecutionReport to map
   * @return the mapped GraphQL StructuredReport object
   */
  private StructuredReport mapStructuredReport(final StructuredExecutionReport structuredReport) {
    StructuredReport structuredReportResult = new StructuredReport();
    structuredReportResult.setType(structuredReport.getType());
    structuredReportResult.setSerializedValue(structuredReport.getSerializedValue());
    structuredReportResult.setContentType(structuredReport.getContentType());
    return structuredReportResult;
  }
}
