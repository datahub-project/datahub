package io.datahubproject.openapi.operations.elastic;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/operations/elasticSearch")
@Slf4j
@Tag(name = "ElasticSearch Raw Operations", description = "An API debugging raw ES documents")
public class ElasticsearchRawController {
  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;
  private final SystemMetadataService systemMetadataService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final EntitySearchService searchService;
  private final GraphService graphService;

  public ElasticsearchRawController(
      OperationContext systemOperationContext,
      SystemMetadataService systemMetadataService,
      TimeseriesAspectService timeseriesAspectService,
      EntitySearchService searchService,
      AuthorizerChain authorizerChain,
      GraphService graphService) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.systemMetadataService = systemMetadataService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.searchService = searchService;
    this.graphService = graphService;
  }

  @PostMapping(path = "/entity/raw", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves raw Elasticsearch documents for the provided URNs. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved raw documents",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access raw documents"),
        @ApiResponse(responseCode = "400", description = "Invalid URN format provided")
      })
  public ResponseEntity<Map<Urn, Map<String, Object>>> getEntityRaw(
      HttpServletRequest request,
      @RequestBody
          @Nonnull
          @Schema(
              description = "Set of URN strings to fetch raw documents for",
              example = "[\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)\"]")
          Set<String> urnStrs) {

    Set<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "getRawEntity",
                    urns.stream().map(Urn::getEntityType).distinct().toList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get raw ES documents", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    return ResponseEntity.ok(searchService.raw(opContext, urns));
  }

  @PostMapping(path = "/systemmetadata/raw", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves raw Elasticsearch system metadata document for the provided URN & aspect. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved raw documents",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access raw documents"),
        @ApiResponse(responseCode = "400", description = "Invalid URN format provided")
      })
  public ResponseEntity<Map<Urn, Map<String, Map<String, Object>>>> getSystemMetadataRaw(
      HttpServletRequest request,
      @RequestBody
          @Nonnull
          @Schema(
              description = "Map of URNs to aspect names to fetch raw documents for",
              example =
                  "{\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)\":[\"status\",\"datasetProperties\"]}")
          Map<String, Set<String>> urnAspects) {

    Set<Urn> urns = urnAspects.keySet().stream().map(UrnUtils::getUrn).collect(Collectors.toSet());

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "getRawSystemMetadata",
                    urns.stream().map(Urn::getEntityType).distinct().toList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get raw ES documents", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    return ResponseEntity.ok(systemMetadataService.raw(opContext, urnAspects));
  }

  @PostMapping(path = "/timeseries/raw", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves raw Elasticsearch timeseries document for the provided URN & aspect. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved raw documents",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access raw documents"),
        @ApiResponse(responseCode = "400", description = "Invalid URN format provided")
      })
  public ResponseEntity<Map<Urn, Map<String, Map<String, Object>>>> getTimeseriesRaw(
      HttpServletRequest request,
      @RequestBody
          @Nonnull
          @Schema(
              description = "Map of URNs to aspect names to fetch raw documents for",
              example =
                  "{\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)\":[\"datasetProfile\"]}")
          Map<String, Set<String>> urnAspects) {

    Set<Urn> urns = urnAspects.keySet().stream().map(UrnUtils::getUrn).collect(Collectors.toSet());

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "getRawTimeseries",
                    urns.stream().map(Urn::getEntityType).distinct().toList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get raw ES documents", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    return ResponseEntity.ok(timeseriesAspectService.raw(opContext, urnAspects));
  }

  @PostMapping(path = "/graph/raw", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      description =
          "Retrieves raw Elasticsearch graph edge document for the provided graph nodes and relationship type. Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved raw documents",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(
            responseCode = "403",
            description = "Caller not authorized to access raw documents"),
        @ApiResponse(responseCode = "400", description = "Invalid URN format provided")
      })
  public ResponseEntity<List<Map<String, Object>>> getGraphRaw(
      HttpServletRequest request,
      @RequestBody
          @Nonnull
          @Schema(
              description =
                  "List of node and relationship tuples. Non-directed, multiple edges may be returned.",
              example =
                  "[{\"a\":\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\", \"b\": \"urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)\", \"relationshipType\": \"DownstreamOf\"}]")
          List<GraphService.EdgeTuple> edgeTuples) {

    Set<Urn> urns =
        edgeTuples.stream()
            .flatMap(tuple -> Stream.of(tuple.getA(), tuple.getB()))
            .map(UrnUtils::getUrn)
            .collect(Collectors.toSet());

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        systemOperationContext.asSession(
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "getRawGraph",
                    urns.stream().map(Urn::getEntityType).distinct().toList()),
            authorizerChain,
            authentication);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      log.error("{} is not authorized to get raw ES documents", actorUrnStr);
      return ResponseEntity.status(HttpStatus.FORBIDDEN).body(null);
    }

    return ResponseEntity.ok(graphService.raw(opContext, edgeTuples));
  }
}
