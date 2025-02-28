package io.datahubproject.openapi.operations.elastic;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.deblock.jsondiff.DiffGenerator;
import com.deblock.jsondiff.matcher.CompositeJsonMatcher;
import com.deblock.jsondiff.matcher.LenientJsonArrayPartialMatcher;
import com.deblock.jsondiff.matcher.LenientJsonObjectPartialMatcher;
import com.deblock.jsondiff.matcher.LenientNumberPrimitivePartialMatcher;
import com.deblock.jsondiff.matcher.StrictPrimitivePartialMatcher;
import com.deblock.jsondiff.viewer.PatchDiffViewer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
@RequestMapping("/openapi/operations/elasticSearch")
@Slf4j
@Tag(
    name = "ElasticSearchOperations",
    description = "An API for managing your elasticsearch instance")
public class OperationsController {
  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;
  private final SystemMetadataService systemMetadataService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final EntitySearchService searchService;
  private final EntityService<?> entityService;
  private final ObjectMapper objectMapper;

  public OperationsController(
      OperationContext systemOperationContext,
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService searchService,
      EntityService<?> entityService,
      AuthorizerChain authorizerChain,
      ObjectMapper objectMapper) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.systemMetadataService = systemMetadataService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.searchService = searchService;
    this.entityService = entityService;
    this.objectMapper = objectMapper;
  }

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @Tag(name = "ElasticSearchOperations")
  @GetMapping(path = "/getTaskStatus", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get Task Status")
  public ResponseEntity<String> getTaskStatus(HttpServletRequest request, String task) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrnStr, request, "getTaskStatus", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorized(opContext, PoliciesConfig.GET_ES_TASK_STATUS_PRIVILEGE)) {
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
  public ResponseEntity<String> getIndexSizes(HttpServletRequest request) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrnStr, request, "getIndexSizes", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.GET_TIMESERIES_INDEX_SIZES_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(String.format(actorUrnStr + " is not authorized to get timeseries index sizes"));
    }

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
      HttpServletRequest request,
      @Parameter(
              name = "query",
              required = true,
              description =
                  "Query to evaluate for specified document, will be applied as an input string to standard search query builder.")
          @RequestParam(value = "query", defaultValue = "*")
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
          @RequestParam(value = "entityName", defaultValue = "dataset")
          @Nonnull
          String entityName,
      @Parameter(name = "scrollId", description = "Scroll ID for pagination.")
          @RequestParam("scrollId")
          @Nullable
          String scrollId,
      @Parameter(
              name = "keepAlive",
              description =
                  "Keep alive time for point in time scroll context"
                      + ", only relevant where point in time is supported.")
          @RequestParam("keepAlive")
          @Nullable
          String keepAlive,
      @Parameter(name = "size", required = true, description = "Page size for pagination.")
          @RequestParam(value = "size", required = false, defaultValue = "1")
          int size,
      @Parameter(name = "filters", description = "Additional filters to apply to query.")
          @RequestParam(value = "filters", required = false)
          @Nullable
          String filters,
      @Parameter(
              name = "sortCriteria",
              required = false,
              description = "Criteria to sort results on.")
          @RequestParam("sortCriteria")
          @Nullable
          List<SortCriterion> sortCriteria,
      @Parameter(name = "searchFlags", description = "Optional configuration flags.")
          @RequestParam(
              value = "searchFlags",
              required = false,
              defaultValue = "{\"fulltext\":true}")
          @Nullable
          String searchFlags)
      throws JsonProcessingException {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext
            .asSession(
                RequestContext.builder()
                    .buildOpenapi(actorUrnStr, request, "explainSearchQuery", entityName),
                authorizerChain,
                authentication)
            .withSearchFlags(
                flags -> {
                  try {
                    return searchFlags == null
                        ? flags
                        : objectMapper.readValue(searchFlags, SearchFlags.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                });

    if (!AuthUtil.isAPIOperationsAuthorized(opContext, PoliciesConfig.ES_EXPLAIN_QUERY_PRIVILEGE)) {
      log.error("{} is not authorized to get explain queries", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    ExplainResponse response =
        searchService.explain(
            opContext,
            query,
            encodeValue(documentId),
            entityName,
            filters == null ? null : objectMapper.readValue(filters, Filter.class),
            sortCriteria,
            scrollId,
            keepAlive,
            size,
            null);

    return ResponseEntity.ok(response);
  }

  @Tag(name = "ElasticSearchOperations")
  @GetMapping(path = "/explainSearchQueryDiff", produces = MediaType.TEXT_PLAIN_VALUE)
  @Operation(summary = "Explain the differences in scoring for 2 documents")
  public ResponseEntity<String> explainSearchQueryDiff(
      HttpServletRequest request,
      @Parameter(
              name = "query",
              required = true,
              description =
                  "Query to evaluate for specified document, will be applied as an input string to standard search query builder.")
          @RequestParam(value = "query", defaultValue = "*")
          @Nonnull
          String query,
      @Parameter(
              name = "documentIdA",
              required = true,
              description = "Document 1st ID to apply explain to.")
          @RequestParam("documentIdA")
          @Nonnull
          String documentIdA,
      @Parameter(
              name = "documentIdB",
              required = true,
              description = "Document 2nd ID to apply explain to.")
          @RequestParam("documentIdB")
          @Nonnull
          String documentIdB,
      @Parameter(
              name = "entityName",
              required = true,
              description = "Name of the entity the document belongs to.")
          @RequestParam(value = "entityName", defaultValue = "dataset")
          @Nonnull
          String entityName,
      @Parameter(name = "scrollId", description = "Scroll ID for pagination.")
          @RequestParam("scrollId")
          @Nullable
          String scrollId,
      @Parameter(
              name = "keepAlive",
              description =
                  "Keep alive time for point in time scroll context"
                      + ", only relevant where point in time is supported.")
          @RequestParam("keepAlive")
          @Nullable
          String keepAlive,
      @Parameter(name = "size", required = true, description = "Page size for pagination.")
          @RequestParam(value = "size", required = false, defaultValue = "1")
          int size,
      @Parameter(name = "filters", description = "Additional filters to apply to query.")
          @RequestParam(value = "filters", required = false)
          @Nullable
          String filters,
      @Parameter(
              name = "sortCriteria",
              required = false,
              description = "Criteria to sort results on.")
          @RequestParam("sortCriteria")
          @Nullable
          List<SortCriterion> sortCriteria,
      @Parameter(name = "searchFlags", description = "Optional configuration flags.")
          @RequestParam(
              value = "searchFlags",
              required = false,
              defaultValue = "{\"fulltext\":true}")
          @Nullable
          String searchFlags)
      throws JsonProcessingException {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        systemOperationContext
            .asSession(
                RequestContext.builder()
                    .buildOpenapi(actorUrnStr, request, "explainSearchQuery", entityName),
                authorizerChain,
                authentication)
            .withSearchFlags(
                flags -> {
                  try {
                    return searchFlags == null
                        ? flags
                        : objectMapper.readValue(searchFlags, SearchFlags.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                });

    if (!AuthUtil.isAPIOperationsAuthorized(opContext, PoliciesConfig.ES_EXPLAIN_QUERY_PRIVILEGE)) {
      log.error("{} is not authorized to get explain queries", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    ExplainResponse responseA =
        searchService.explain(
            opContext,
            query,
            encodeValue(documentIdA),
            entityName,
            filters == null ? null : objectMapper.readValue(filters, Filter.class),
            sortCriteria,
            scrollId,
            keepAlive,
            size,
            null);

    ExplainResponse responseB =
        searchService.explain(
            opContext,
            query,
            encodeValue(documentIdB),
            entityName,
            filters == null ? null : objectMapper.readValue(filters, Filter.class),
            sortCriteria,
            scrollId,
            keepAlive,
            size,
            null);

    String a = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseA);
    String b = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseB);

    CompositeJsonMatcher fullLenient =
        new CompositeJsonMatcher(
            new LenientJsonArrayPartialMatcher(), // comparing array using lenient mode (ignore
            // array order and extra items)
            new LenientJsonObjectPartialMatcher(), // comparing object using lenient mode (ignoring
            // extra properties)
            new LenientNumberPrimitivePartialMatcher(
                new StrictPrimitivePartialMatcher()) // comparing primitive types and manage numbers
            // (100.00 == 100)
            );

    // generate a diff
    final var jsondiff = DiffGenerator.diff(a, b, fullLenient);
    PatchDiffViewer patch = PatchDiffViewer.from(jsondiff);

    return ResponseEntity.ok(patch.toString());
  }

  private static String encodeValue(String value) {
    if (value.startsWith("urn:li:")) {
      return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
    return value;
  }

  @Tag(name = "RestoreIndices")
  @GetMapping(path = "/restoreIndices", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Restore ElasticSearch indices from primary storage based on URNs.")
  public ResponseEntity<List<RestoreIndicesResult>> restoreIndices(
      HttpServletRequest request,
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
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "restoreIndices", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIOperationsAuthorized(opContext, PoliciesConfig.RESTORE_INDICES_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }

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
      HttpServletRequest request,
      @RequestParam(required = false, name = "aspectNames") @Nullable Set<String> aspectNames,
      @RequestParam(required = false, name = "batchSize", defaultValue = "100") @Nullable
          Integer batchSize,
      @RequestBody @Nonnull Set<String> urns)
      throws RemoteInvocationException, URISyntaxException {

    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "restoreIndices", List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIOperationsAuthorized(opContext, PoliciesConfig.RESTORE_INDICES_PRIVILEGE)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }

    return ResponseEntity.of(
        Optional.of(
            entityService.restoreIndices(
                opContext,
                urns.stream().map(UrnUtils::getUrn).collect(Collectors.toSet()),
                aspectNames,
                batchSize)));
  }
}
