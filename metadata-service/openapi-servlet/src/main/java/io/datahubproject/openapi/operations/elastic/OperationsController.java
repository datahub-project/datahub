package io.datahubproject.openapi.operations.elastic;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.openapi.util.ElasticsearchUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.client.tasks.GetTaskResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
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
  private final AuthorizerChain authorizerChain;

  @Value("${authorization.restApiAuthorization:false}")
  private boolean restApiAuthorizationEnabled;

  private final SystemMetadataService systemMetadataService;
  private final TimeseriesAspectService timeseriesAspectService;

  private final EntitySearchService searchService;

  public OperationsController(
      AuthorizerChain authorizerChain,
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService searchService) {
    this.authorizerChain = authorizerChain;
    this.systemMetadataService = systemMetadataService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.searchService = searchService;
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
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.GET_ES_TASK_STATUS_PRIVILEGE.getType()))));
    if (restApiAuthorizationEnabled
        && !AuthUtil.isAuthorizedForResources(
            authorizerChain, actorUrnStr, List.of(java.util.Optional.empty()), orGroup)) {
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
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.GET_TIMESERIES_INDEX_SIZES_PRIVILEGE.getType()))));
    if (restApiAuthorizationEnabled
        && !AuthUtil.isAuthorizedForResources(
            authorizerChain, actorUrnStr, List.of(java.util.Optional.empty()), orGroup)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(String.format(actorUrnStr + " is not authorized to get timeseries index sizes"));
    }
    List<TimeseriesIndexSizeResult> indexSizeResults = timeseriesAspectService.getIndexSizes();
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
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.ES_EXPLAIN_QUERY_PRIVILEGE.getType()))));
    if (restApiAuthorizationEnabled
        && !AuthUtil.isAuthorizedForResources(
            authorizerChain, actorUrnStr, List.of(java.util.Optional.empty()), orGroup)) {
      log.error("{} is not authorized to get timeseries index sizes", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }
    ExplainResponse response =
        searchService.explain(
            query,
            documentId,
            entityName,
            filters,
            sortCriterion,
            searchFlags,
            scrollId,
            keepAlive,
            size,
            facets);

    return ResponseEntity.ok(response);
  }
}
