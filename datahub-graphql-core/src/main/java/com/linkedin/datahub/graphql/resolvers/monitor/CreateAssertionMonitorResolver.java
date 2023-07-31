package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AssertionEvaluationParametersInput;
import com.linkedin.datahub.graphql.generated.AuditLogSpecInput;
import com.linkedin.datahub.graphql.generated.CreateAssertionMonitorInput;
import com.linkedin.datahub.graphql.generated.CronScheduleInput;
import com.linkedin.datahub.graphql.generated.DatasetFreshnessAssertionParametersInput;
import com.linkedin.datahub.graphql.generated.FreshnessFieldSpecInput;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.AuditLogSpec;
import com.linkedin.monitor.DatasetFreshnessAssertionParameters;
import com.linkedin.monitor.DatasetFreshnessSourceType;
import com.linkedin.assertion.FreshnessFieldKind;
import com.linkedin.assertion.FreshnessFieldSpec;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.monitor.MonitorUtils.*;


@Slf4j
public class CreateAssertionMonitorResolver implements DataFetcher<CompletableFuture<Monitor>> {

  private final MonitorService _monitorService;

  public CreateAssertionMonitorResolver(@Nonnull final MonitorService monitorService) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
  }

  @Override
  public CompletableFuture<Monitor> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateAssertionMonitorInput
        input = ResolverUtils.bindArgument(environment.getArgument("input"), CreateAssertionMonitorInput.class);
    final Urn assertionUrn = UrnUtils.getUrn(input.getAssertionUrn());
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());

    return CompletableFuture.supplyAsync(() -> {

      if (isAuthorizedToUpdateEntityMonitors(entityUrn, context)) {

        try {
          // First create the new monitor
          final Urn monitorUrn = _monitorService.createAssertionMonitor(
              entityUrn,
              assertionUrn,
              createCronSchedule(input.getSchedule()),
              createAssertionEvaluationParameters(input.getParameters()),
              context.getAuthentication()
          );

          // Then, return the new monitor
          return MonitorMapper.map(_monitorService.getMonitorEntityResponse(monitorUrn, context.getAuthentication()));
        } catch (Exception e) {
          log.error("Failed to create Assertion monitor!", e);
          throw new DataHubGraphQLException("Failed to create Assertion Monitor! An unknown error occurred.", DataHubGraphQLErrorCode.SERVER_ERROR);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private CronSchedule createCronSchedule(@Nonnull final CronScheduleInput input) {
    final CronSchedule result = new CronSchedule();
    result.setCron(input.getCron());
    result.setTimezone(input.getTimezone());
    return result;
  }

  private AssertionEvaluationParameters createAssertionEvaluationParameters(@Nonnull final AssertionEvaluationParametersInput input) {
    final AssertionEvaluationParameters result = new AssertionEvaluationParameters();
    result.setType(AssertionEvaluationParametersType.valueOf(input.getType().toString()));

    if (AssertionEvaluationParametersType.DATASET_FRESHNESS.equals(result.getType()) && input.getDatasetFreshnessParameters() != null) {
      result.setDatasetFreshnessParameters(createDatasetFreshnessParameters(input.getDatasetFreshnessParameters()));
    } else {
      throw new DataHubGraphQLException(
          "Invalid input. Dataset FRESHNESS Parameters are required when type is DATASET_FRESHNESS.",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    return result;
  }

  private DatasetFreshnessAssertionParameters createDatasetFreshnessParameters(@Nonnull final DatasetFreshnessAssertionParametersInput input) {
    final DatasetFreshnessAssertionParameters result = new DatasetFreshnessAssertionParameters();
    result.setSourceType(DatasetFreshnessSourceType.valueOf(input.getSourceType().toString()));
    if (DatasetFreshnessSourceType.AUDIT_LOG.equals(result.getSourceType())) {
      if (input.getAuditLog() != null) {
        result.setAuditLog(createAuditLogSpec(input.getAuditLog()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Audit Log info is required if type FRESHNESS source type is AUDIT_LOG.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }

    }
    if (DatasetFreshnessSourceType.FIELD_VALUE.equals(result.getSourceType())) {
      if (input.getField() != null) {
        result.setField(createFreshnessFieldSpec(input.getField()));
      } else {
        throw new DataHubGraphQLException(
            "Invalid input. Field info is required if type FRESHNESS source type is FIELD_VALUE.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    return result;
   }

   private AuditLogSpec createAuditLogSpec(@Nonnull final AuditLogSpecInput input) {
    final AuditLogSpec result = new AuditLogSpec();
    if (input.getOperationTypes() != null) {
      result.setOperationTypes(new StringArray(input.getOperationTypes()));
    }
    if (input.getUserName() != null) {
      result.setUserName(input.getUserName());
    }
    return result;
  }

  private FreshnessFieldSpec createFreshnessFieldSpec(@Nonnull final FreshnessFieldSpecInput input) {
    final FreshnessFieldSpec result = new FreshnessFieldSpec();
    result.setType(input.getType());
    result.setNativeType(input.getNativeType(), SetMode.IGNORE_NULL);
    result.setPath(input.getPath(), SetMode.IGNORE_NULL);

    if (input.getKind() != null) {
        result.setKind(FreshnessFieldKind.valueOf(input.getKind().toString()));
    } else {
        throw new DataHubGraphQLException(
            "Invalid input. Freshness Field Kind info is required if type Freshness source type is FIELD_VALUE.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  
    return result;
  }
}