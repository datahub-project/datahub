package com.linkedin.metadata.resources.operations;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.datahub.authorization.AuthUtil.isAPIOperationsAuthorized;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import com.linkedin.timeseries.TimeseriesIndexSizeResultArray;
import com.linkedin.timeseries.TimeseriesIndicesSizesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.opensearch.client.tasks.GetTaskResponse;

/** Endpoints for performing maintenance operations */
@Slf4j
@RestLiCollection(name = "operations", namespace = "com.linkedin.operations")
public class OperationsResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {
  private static final String ACTION_GET_INDEX_SIZES = "getIndexSizes";
  public static final String ACTION_RESTORE_INDICES = "restoreIndices";
  private static final String ACTION_TRUNCATE_TIMESERIES_ASPECT = "truncateTimeseriesAspect";
  private static final String ACTION_GET_ES_TASK_STATUS = "getEsTaskStatus";
  private static final String PARAM_BATCH_SIZE = "batchSize";
  private static final String PARAM_ASPECT = "aspect";
  private static final String PARAM_IS_DRY_RUN = "dryRun";
  private static final String PARAM_END_TIME_MILLIS = "endTimeMillis";
  private static final String PARAM_TIMEOUT_SECONDS = "timeoutSeconds";
  private static final String PARAM_NODE_ID = "nodeId";
  private static final String PARAM_TASK_ID = "taskId";
  private static final String PARAM_TASK = "task";
  private static final String PARAM_FORCE_DELETE_BY_QUERY = "forceDeleteByQuery";
  private static final String PARAM_FORCE_REINDEX = "forceReindex";

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Inject
  @Named("elasticSearchSystemMetadataService")
  private SystemMetadataService _systemMetadataService;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

    @Inject
    @Named("systemOperationContext")
    private OperationContext systemOperationContext;

  public OperationsResource() {}

  @VisibleForTesting
  OperationsResource(OperationContext systemOperationContext, TimeseriesAspectService timeseriesAspectService) {
    this._timeseriesAspectService = timeseriesAspectService;
    this.systemOperationContext = systemOperationContext;
    this._authorizer = systemOperationContext.getAuthorizationContext().getAuthorizer();
  }

  @Action(name = ACTION_RESTORE_INDICES)
  @Nonnull
  @WithSpan
  public Task<String> restoreIndices(
      @ActionParam(PARAM_ASPECT) @Optional @Nonnull String aspectName,
      @ActionParam(PARAM_URN) @Optional @Nullable String urn,
      @ActionParam(PARAM_URN_LIKE) @Optional @Nullable String urnLike,
      @ActionParam("start") @Optional @Nullable Integer start,
      @ActionParam("batchSize") @Optional @Nullable Integer batchSize,
      @ActionParam("limit") @Optional @Nullable Integer limit,
      @ActionParam("gePitEpochMs") @Optional @Nullable Long gePitEpochMs,
      @ActionParam("lePitEpochMs") @Optional @Nullable Long lePitEpochMs) {
    return RestliUtils.toTask(systemOperationContext,
      () ->  Utils.restoreIndices(systemOperationContext, getContext(),
                  aspectName, urn, urnLike, start, batchSize, limit, gePitEpochMs, lePitEpochMs, _authorizer, _entityService),
        MetricRegistry.name(this.getClass(), "restoreIndices"));
  }

  @VisibleForTesting
  static boolean isTaskIdValid(String task) {
    if (task.matches("^[a-zA-Z0-9-_]+:[0-9]+$")) {
      try {
        return Long.parseLong(task.split(":")[1]) != 0;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  @Action(name = ACTION_GET_ES_TASK_STATUS)
  @Nonnull
  @WithSpan
  public Task<String> getTaskStatus(
      @ActionParam(PARAM_NODE_ID) @Optional String nodeId,
      @ActionParam(PARAM_TASK_ID) @Optional("0") long taskId,
      @ActionParam(PARAM_TASK) @Optional String task) {
    return RestliUtils.toTask(systemOperationContext,
        () -> {

          final Authentication auth = AuthenticationContext.getAuthentication();
          final OperationContext opContext = OperationContext.asSession(
                  systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                          ACTION_GET_ES_TASK_STATUS), _authorizer, auth, true);

          if (!isAPIOperationsAuthorized(
                  opContext,
                  PoliciesConfig.GET_ES_TASK_STATUS_PRIVILEGE)) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get ES task status");
          }
          boolean taskSpecified = task != null;
          boolean nodeAndTaskIdSpecified = nodeId != null && taskId > 0;
          if (!taskSpecified && !nodeAndTaskIdSpecified) {
            throw new RestLiServiceException(
                HttpStatus.S_400_BAD_REQUEST,
                "Please specify either Node ID + task ID OR composite task parameters");
          }

          if (taskSpecified
              && nodeAndTaskIdSpecified
              && !task.equals(String.format("%s:%d", nodeId, taskId))) {
            throw new RestLiServiceException(
                HttpStatus.S_400_BAD_REQUEST,
                "Please specify only one of Node ID + task ID OR composite task parameters");
          }

          if (taskSpecified && !isTaskIdValid(task)) {
            throw new RestLiServiceException(
                HttpStatus.S_400_BAD_REQUEST,
                String.format(
                    "Task should be in the form nodeId:taskId e.g. aB1cdEf2GHIJKLMnoPQr3S:123456 (got %s)",
                    task));
          }

          String nodeIdToQuery = nodeAndTaskIdSpecified ? nodeId : task.split(":")[0];
          long taskIdToQuery = nodeAndTaskIdSpecified ? taskId : Long.parseLong(task.split(":")[1]);
          java.util.Optional<GetTaskResponse> res =
              _systemMetadataService.getTaskStatus(nodeIdToQuery, taskIdToQuery);
          JSONObject j = new JSONObject();
          if (res.isEmpty()) {
            j.put(
                "error",
                String.format("Could not get task status for %s:%d", nodeIdToQuery, taskIdToQuery));
            return j.toString();
          }
          GetTaskResponse resp = res.get();
          j.put("completed", resp.isCompleted());
          j.put("taskId", res.get().getTaskInfo().getTaskId());
          j.put("status", res.get().getTaskInfo().getStatus());
          j.put("runTimeNanos", res.get().getTaskInfo().getRunningTimeNanos());
          return j.toString();
        },
        MetricRegistry.name(this.getClass(), "getTaskStatus"));
  }

  @Action(name = ACTION_GET_INDEX_SIZES)
  @Nonnull
  @WithSpan
  public Task<TimeseriesIndicesSizesResult> getIndexSizes() {
    return RestliUtils.toTask(systemOperationContext,
        () -> {

            final Authentication auth = AuthenticationContext.getAuthentication();
          final OperationContext opContext = OperationContext.asSession(
                  systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                          ACTION_GET_INDEX_SIZES, List.of()), _authorizer, auth, true);

          if (!isAPIOperationsAuthorized(
                  opContext,
                  PoliciesConfig.GET_TIMESERIES_INDEX_SIZES_PRIVILEGE)) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get index sizes.");
          }

            TimeseriesIndicesSizesResult result = new TimeseriesIndicesSizesResult();
          result.setIndexSizes(
              new TimeseriesIndexSizeResultArray(_timeseriesAspectService.getIndexSizes(opContext)));
          return result;
        },
        MetricRegistry.name(this.getClass(), "getIndexSizes"));
  }

  @VisibleForTesting
  String executeTruncateTimeseriesAspect(
      @Nonnull String entityType,
      @Nonnull String aspectName,
      @Nonnull Long endTimeMillis,
      @Nonnull Boolean dryRun,
      @Nullable Integer batchSize,
      @Nullable Long timeoutSeconds,
      @Nullable Boolean forceDeleteByQuery,
      @Nullable Boolean forceReindex) {

      final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "executeTruncateTimeseriesAspect", entityType), _authorizer, auth, true);

    if (!isAPIAuthorized(
            opContext,
            PoliciesConfig.TRUNCATE_TIMESERIES_INDEX_PRIVILEGE)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to truncate timeseries index");
    }

    if (forceDeleteByQuery != null && forceDeleteByQuery.equals(forceReindex)) {
      return "please only set forceReindex OR forceDeleteByQuery flags";
    }

    List<Criterion> criteria = new ArrayList<>();
    criteria.add(
        buildCriterion(
            "timestampMillis", Condition.LESS_THAN_OR_EQUAL_TO, String.valueOf(endTimeMillis)));

    final Filter filter = QueryUtils.getFilterFromCriteria(criteria);
    long numToDelete = _timeseriesAspectService.countByFilter(opContext, entityType, aspectName, filter);
    long totalNum = _timeseriesAspectService.countByFilter(opContext, entityType, aspectName, new Filter());

    String deleteSummary =
        String.format(
            "Delete %d out of %d rows (%.2f%%). ",
            numToDelete, totalNum, ((double) numToDelete) / totalNum * 100);
    boolean reindex =
        !(forceDeleteByQuery != null && forceDeleteByQuery)
            && ((forceReindex != null && forceReindex) || numToDelete > (totalNum / 2));

    if (reindex) {
      deleteSummary += "Reindexing the aspect without the deleted records. ";
    } else {
      deleteSummary += "Issuing a delete by query request. ";
    }

    if (dryRun) {
      deleteSummary += "This was a dry run. Run with dryRun = false to execute.";
    }

    log.info(deleteSummary);

    if (dryRun) {
      return deleteSummary;
    } else {
      BatchWriteOperationsOptions options = new BatchWriteOperationsOptions();
      if (batchSize != null) {
        options.setBatchSize(batchSize);
      }
      if (timeoutSeconds != null) {
        options.setTimeoutSeconds(timeoutSeconds);
      }

      if (reindex) {
        // need to invert query to retain only the ones that do NOT meet the criterion from the
        // count
        List<Criterion> reindexCriteria = new ArrayList<>();
        reindexCriteria.add(
            buildCriterion(
                "timestampMillis", Condition.GREATER_THAN, String.valueOf(endTimeMillis)));

        final Filter reindexFilter = QueryUtils.getFilterFromCriteria(reindexCriteria);
        String taskId =
            _timeseriesAspectService.reindexAsync(opContext, entityType, aspectName, reindexFilter, options);
        log.info("reindex request submitted with ID " + taskId);
        return taskId;
      } else {
        String taskId =
            _timeseriesAspectService.deleteAspectValuesAsync(opContext,
                    entityType, aspectName, filter, options);
        log.info("delete by query request submitted with ID " + taskId);
        return taskId;
      }
    }
  }

  @Action(name = ACTION_TRUNCATE_TIMESERIES_ASPECT)
  @Nonnull
  @WithSpan
  public Task<String> truncateTimeseriesAspect(
      @ActionParam(PARAM_ENTITY_TYPE) @Nonnull String entityType,
      @ActionParam(PARAM_ASPECT) @Nonnull String aspectName,
      @ActionParam(PARAM_END_TIME_MILLIS) @Nonnull Long endTimeMillis,
      @ActionParam(PARAM_IS_DRY_RUN) @Optional("true") @Nonnull Boolean dryRun,
      @ActionParam(PARAM_BATCH_SIZE) @Optional @Nullable Integer batchSize,
      @ActionParam(PARAM_TIMEOUT_SECONDS) @Optional @Nullable Long timeoutSeconds,
      @ActionParam(PARAM_FORCE_DELETE_BY_QUERY) @Optional @Nullable Boolean forceDeleteByQuery,
      @ActionParam(PARAM_FORCE_REINDEX) @Optional @Nullable Boolean forceReindex) {
    return RestliUtils.toTask(systemOperationContext,
        () ->
            executeTruncateTimeseriesAspect(
                entityType,
                aspectName,
                endTimeMillis,
                dryRun,
                batchSize,
                timeoutSeconds,
                forceDeleteByQuery,
                forceReindex),
        MetricRegistry.name(this.getClass(), "truncateTimeseriesAspect"));
  }
}
