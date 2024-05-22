package io.datahubproject.openapi.operations.elastic;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.util.ElasticsearchUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.client.tasks.GetTaskResponse;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operations/elasticSearch")
@Slf4j
@Tag(
    name = "ElasticSearchOperations",
    description = "An API for managing your elasticsearch instance")
public class OperationsController {
  private final Authorizer authorizerChain;
  private final OperationContext systemOperationContext;
  private final SystemMetadataService systemMetadataService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final EntitySearchService searchService;
  private final EntityService<?> entityService;

  public OperationsController(
      OperationContext systemOperationContext,
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService searchService,
      EntityService<?> entityService) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = systemOperationContext.getAuthorizerContext().getAuthorizer();
    this.systemMetadataService = systemMetadataService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.searchService = searchService;
    this.entityService = entityService;
  }

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @Tag(name = "ElasticSearchOperations")
  @GetMapping(path = "/getTaskStatus", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get Task Status")
  public ResponseEntity<String> getTaskStatus(String task) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    if (!AuthUtil.isAPIAuthorized(
        authentication, authorizerChain, PoliciesConfig.GET_ES_TASK_STATUS_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(String.format(actorUrnStr + " is not authorized to get ElasticSearch task status"));
    }
    if (!ElasticsearchUtils.isTaskIdValid(task)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(
              String.format(
                  "Task ID should be in the form nodeId:taskId e.g. aB1cdEf2GHI-JKLMnoPQr3:123456 (got %s)",
                  task));
    }
    String nodeIdToQuery = task.split(":")[0];
    long taskIdToQuery = Long.parseLong(task.split(":")[1]);
    java.util.Optional<GetTaskResponse> res =
        systemMetadataService.getTaskStatus(nodeIdToQuery, taskIdToQuery);
    if (res.isEmpty()) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(String.format("Could not get task status for %s:%d", nodeIdToQuery, taskIdToQuery));
    }
    GetTaskResponse resp = res.get();
    JSONObject j = new JSONObject();
    j.put("completed", resp.isCompleted());
    j.put("taskId", res.get().getTaskInfo().getTaskId());
    j.put("status", res.get().getTaskInfo().getStatus());
    j.put("runTimeNanos", res.get().getTaskInfo().getRunningTimeNanos());
    return ResponseEntity.ok(j.toString());
  }

  @Tag(name = "ElasticSearchOperations")
  @GetMapping(path = "/getIndexSizes", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get Index Sizes")
  public ResponseEntity<String> getIndexSizes() {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    if (!AuthUtil.isAPIAuthorized(
        authentication, authorizerChain, PoliciesConfig.GET_TIMESERIES_INDEX_SIZES_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(String.format(actorUrnStr + " is not authorized to get timeseries index sizes"));
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("getIndexSizes", List.of()),
            authorizerChain,
            authentication,
            true);

    List<TimeseriesIndexSizeResult> indexSizeResults =
        timeseriesAspectService.getIndexSizes(opContext);
    JSONObject j = new JSONObject();
    j.put(
        "sizes",
        indexSizeResults.stream()
            .map(
                timeseriesIndexSizeResult ->
                    new JSONObject()
                        .put("aspectName", timeseriesIndexSizeResult.getAspectName())
                        .put("entityName", timeseriesIndexSizeResult.getEntityName())
                        .put("indexName", timeseriesIndexSizeResult.getIndexName())
                        .put("sizeMb", timeseriesIndexSizeResult.getSizeInMb()))
            .collect(Collectors.toList()));
    return ResponseEntity.ok(j.toString());
  }

  @Tag(name = "ElasticSearchOperations")
  @GetMapping(path = "/explainSearchQuery", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Explain Search Query")
  public ResponseEntity<ExplainResponse> explainSearchQuery(
      @Parameter(
              name = "query",
              required = true,
              description =
                  "Query to evaluate for specified document, will be applied as an input string to standard search query builder.")
          @RequestParam("query")
          @Nonnull
          String query,
      @Parameter(
              name = "documentId",
              required = true,
              description = "Document ID to apply explain to.")
          @RequestParam("documentId")
          @Nonnull
          String documentId,
      @Parameter(
              name = "entityName",
              required = true,
              description = "Name of the entity the document belongs to.")
          @RequestParam("entityName")
          @Nonnull
          String entityName,
      @Parameter(name = "scrollId", required = false, description = "Scroll ID for pagination.")
          @RequestParam("scrollId")
          @Nullable
          String scrollId,
      @Parameter(
              name = "keepAlive",
              required = false,
              description =
                  "Keep alive time for point in time scroll context"
                      + ", only relevant where point in time is supported.")
          @RequestParam("keepAlive")
          @Nullable
          String keepAlive,
      @Parameter(name = "size", required = true, description = "Page size for pagination.")
          @RequestParam("size")
          int size,
      @Parameter(
              name = "filters",
              required = false,
              description = "Additional filters to apply to query.")
          @RequestParam("filters")
          @Nullable
          Filter filters,
      @Parameter(
              name = "sortCriterion",
              required = false,
              description = "Criterion to sort results on.")
          @RequestParam("sortCriterion")
          @Nullable
          SortCriterion sortCriterion,
      @Parameter(
              name = "searchFlags",
              required = false,
              description = "Optional configuration flags.")
          @RequestParam("searchFlags")
          @Nullable
          SearchFlags searchFlags,
      @Parameter(
              name = "facets",
              required = false,
              description = "List of facet fields for aggregations.")
          @RequestParam("facets")
          @Nullable
          List<String> facets) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    if (!AuthUtil.isAPIAuthorized(
        authentication, authorizerChain, PoliciesConfig.ES_EXPLAIN_QUERY_PRIVILEGE)) {
      log.error("{} is not authorized to get timeseries index sizes", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }
    OperationContext opContext =
        systemOperationContext
            .asSession(
                RequestContext.builder().buildOpenapi("explainSearchQuery", entityName),
                authorizerChain,
                authentication)
            .withSearchFlags(flags -> searchFlags);

    ExplainResponse response =
        searchService.explain(
            opContext,
            query,
            documentId,
            entityName,
            filters,
            sortCriterion,
            scrollId,
            keepAlive,
            size,
            facets);

    return ResponseEntity.ok(response);
  }

  @Tag(name = "RestoreIndices")
  @GetMapping(path = "/restoreIndices", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Restore ElasticSearch indices from primary storage based on URNs.")
  public ResponseEntity<List<RestoreIndicesResult>> restoreIndices(
      @RequestParam(required = false, name = "aspectName") @Nullable String aspectName,
      @RequestParam(required = false, name = "urn") @Nullable String urn,
      @RequestParam(required = false, name = "urnLike") @Nullable String urnLike,
      @RequestParam(required = false, name = "batchSize", defaultValue = "500") @Nullable
          Integer batchSize,
      @RequestParam(required = false, name = "start", defaultValue = "0") @Nullable Integer start,
      @RequestParam(required = false, name = "limit", defaultValue = "0") @Nullable Integer limit,
      @RequestParam(required = false, name = "gePitEpochMs", defaultValue = "0") @Nullable
          Long gePitEpochMs,
      @RequestParam(required = false, name = "lePitEpochMs") @Nullable Long lePitEpochMs) {

    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorized(
        authentication, authorizerChain, PoliciesConfig.RESTORE_INDICES_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("restoreIndices", List.of()),
            authorizerChain,
            authentication,
            true);

    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .aspectName(aspectName)
            .urnLike(urnLike)
            .urn(
                Optional.ofNullable(urn)
                    .map(urnStr -> UrnUtils.getUrn(urnStr).toString())
                    .orElse(null))
            .start(start)
            .batchSize(batchSize)
            .limit(limit)
            .gePitEpochMs(gePitEpochMs)
            .lePitEpochMs(lePitEpochMs);

    return ResponseEntity.of(Optional.of(entityService.restoreIndices(opContext, args, log::info)));
  }

  @Tag(name = "RestoreIndices")
  @PostMapping(path = "/restoreIndices", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Restore ElasticSearch indices from primary storage based on URNs.")
  public ResponseEntity<List<RestoreIndicesResult>> restoreIndices(
      @RequestParam(required = false, name = "aspectNames") @Nullable Set<String> aspectNames,
      @RequestParam(required = false, name = "batchSize", defaultValue = "100") @Nullable
          Integer batchSize,
      @RequestBody @Nonnull Set<String> urns)
      throws RemoteInvocationException, URISyntaxException {

    Authentication authentication = AuthenticationContext.getAuthentication();
    if (!AuthUtil.isAPIAuthorized(
        authentication, authorizerChain, PoliciesConfig.RESTORE_INDICES_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi("restoreIndices", List.of()),
            authorizerChain,
            authentication,
            true);

    return ResponseEntity.of(
        Optional.of(
            entityService.restoreIndices(
                opContext,
                urns.stream().map(UrnUtils::getUrn).collect(Collectors.toSet()),
                aspectNames,
                batchSize)));
  }
}
