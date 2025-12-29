package io.datahubproject.openapi.operations.consistency;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.SystemMetadataFilter;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.config.ConsistencyChecksConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyCheckInfo;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyCheckRequest;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyCheckResult;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyChecksResponse;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyFixIssuesRequest;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyFixIssuesResult;
import io.datahubproject.openapi.operations.consistency.models.ConsistencyFixRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Generic controller for checking and fixing consistency issues across all entity types.
 *
 * <p>This API is registry-driven - available entity types and checks are determined by what's
 * registered in the {@link com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry}.
 *
 * <p>Endpoints:
 *
 * <ul>
 *   <li>GET /consistency/checks - List all registered checks (with optional filters)
 *   <li>GET /consistency/checks/{checkId} - Get details about a specific check
 *   <li>GET /consistency/entities/{urn} - Check a single entity
 *   <li>POST /consistency/check - Run batch consistency checks
 *   <li>POST /consistency/fix - Check and fix in one operation
 *   <li>POST /consistency/fix-issues - Fix specific issues from a check response
 * </ul>
 *
 * <p>Requires MANAGE_SYSTEM_OPERATIONS_PRIVILEGE authorization.
 */
@RestController
@RequestMapping("/openapi/operations/consistency")
@Slf4j
@Validated
@Tag(
    name = "Entity Consistency",
    description =
        "APIs for checking and fixing consistency issues across entity types. "
            + "Entity types and checks are registry-driven - use GET /consistency/checks to discover available options.")
public class ConsistencyController {

  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;
  private final ConsistencyService consistencyService;
  private final ConsistencyChecksConfiguration consistencyChecksConfig;

  public ConsistencyController(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      ConsistencyService consistencyService,
      DataHubAppConfiguration appConfig) {
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.consistencyService = consistencyService;
    this.consistencyChecksConfig = appConfig.getConsistencyChecks();
  }

  // ============================================================================
  // Discovery Endpoints
  // ============================================================================

  @GetMapping(path = "/checks", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "List all registered consistency checks",
      description =
          "Returns information about all registered checks with optional filtering by entity type. "
              + "Use includeOnDemand=true to also see on-demand checks that don't run by default. "
              + "Parse distinct entityType values from response to discover available entity types.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "List of consistency checks",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ConsistencyChecksResponse.class))),
        @ApiResponse(responseCode = "403", description = "Forbidden - insufficient privileges")
      })
  public ResponseEntity<ConsistencyChecksResponse> listChecks(
      HttpServletRequest request,
      @Parameter(description = "Filter to checks for this entity type", example = "assertion")
          @RequestParam(value = "entityType", required = false)
          String entityType,
      @Parameter(description = "Include on-demand only checks in results")
          @RequestParam(value = "includeOnDemand", defaultValue = "false")
          boolean includeOnDemand) {

    getOperationContext(request, "listChecks");
    try {
      List<ConsistencyCheck> allChecks;
      if (entityType != null && !entityType.isEmpty()) {
        allChecks =
            includeOnDemand
                ? consistencyService.getCheckRegistry().getByEntityType(entityType)
                : consistencyService.getCheckRegistry().getDefaultByEntityType(entityType);
      } else {
        allChecks =
            includeOnDemand
                ? consistencyService.getCheckRegistry().getAll()
                : consistencyService.getCheckRegistry().getDefaultChecks();
      }

      List<ConsistencyCheckInfo> checks = ConsistencyCheckInfo.from(allChecks);

      return ResponseEntity.ok(ConsistencyChecksResponse.builder().checks(checks).build());
    } catch (Exception e) {
      log.error("Error listing consistency checks: {}", e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  @GetMapping(path = "/checks/{checkId}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get details about a specific check",
      description = "Returns detailed information about a single consistency check by its ID.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Check details",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ConsistencyCheckInfo.class))),
        @ApiResponse(responseCode = "403", description = "Forbidden - insufficient privileges"),
        @ApiResponse(responseCode = "404", description = "Check not found")
      })
  public ResponseEntity<ConsistencyCheckInfo> getCheck(
      HttpServletRequest request,
      @Parameter(description = "Check ID", example = "assertion-entity-not-found", required = true)
          @PathVariable("checkId")
          @Nonnull
          String checkId) {

    getOperationContext(request, "getCheck");
    try {
      Optional<ConsistencyCheck> check = consistencyService.getCheckRegistry().getById(checkId);
      if (check.isEmpty()) {
        return ResponseEntity.notFound().build();
      }
      return ResponseEntity.ok(ConsistencyCheckInfo.from(check.get()));
    } catch (Exception e) {
      log.error("Error getting check {}: {}", checkId, e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  // ============================================================================
  // Single Entity Check
  // ============================================================================

  @GetMapping(path = "/entities/{urn}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Check a single entity",
      description =
          "Runs consistency checks on a single entity and returns any issues found. "
              + "Entity type is derived from the URN automatically.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "List of consistency issues found",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema =
                        @Schema(
                            implementation =
                                io.datahubproject.openapi.operations.consistency.models
                                    .ConsistencyIssue.class))),
        @ApiResponse(responseCode = "400", description = "Bad request - invalid URN format"),
        @ApiResponse(responseCode = "403", description = "Forbidden - insufficient privileges"),
        @ApiResponse(responseCode = "404", description = "Entity not found")
      })
  public ResponseEntity<
          List<io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue>>
      checkEntity(
          HttpServletRequest request,
          @Parameter(
                  description = "URL-encoded URN of the entity to check",
                  example = "urn:li:assertion:abc123",
                  required = true)
              @PathVariable("urn")
              @Nonnull
              String urnStr,
          @Parameter(
                  description =
                      "Comma-separated check IDs to run. If omitted, runs all default checks for the entity's type.",
                  example = "assertion-entity-not-found,assertion-monitor-missing")
              @RequestParam(value = "checkIds", required = false)
              String checkIdsParam) {

    OperationContext opContext = getOperationContext(request, "checkEntity");
    try {
      Urn urn = UrnUtils.getUrn(urnStr);

      List<String> checkIds =
          checkIdsParam != null && !checkIdsParam.isEmpty()
              ? Arrays.asList(checkIdsParam.split(","))
              : null;

      // Use unified batch API with URN filter
      CheckResult result =
          consistencyService.checkBatch(
              opContext, CheckBatchRequest.builder().urns(Set.of(urn)).checkIds(checkIds).build());

      if (result.getEntitiesScanned() == 0) {
        // URN not found in ES index
        return ResponseEntity.notFound().build();
      }

      return ResponseEntity.ok(
          io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue.from(
              result.getIssues()));
    } catch (IllegalArgumentException e) {
      log.warn("Invalid URN format: {}", urnStr, e);
      return ResponseEntity.badRequest().build();
    } catch (Exception e) {
      log.error("Error checking entity {}: {}", urnStr, e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  // ============================================================================
  // Batch Check
  // ============================================================================

  @PostMapping(
      path = "/check",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Run batch consistency checks",
      description =
          "Scans entities of the specified type for consistency issues. "
              + "Supports pagination via scrollId. "
              + "Use GET /consistency/checks to discover available entity types and check IDs.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Check completed successfully",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ConsistencyCheckResult.class),
                    examples =
                        @ExampleObject(
                            value =
                                """
                    {
                      "entitiesScanned": 100,
                      "issuesFound": 3,
                      "issues": [],
                      "scrollId": "eyJzb3J0IjpbMTcwMjk1...."
                    }
                    """))),
        @ApiResponse(
            responseCode = "400",
            description = "Bad request - invalid or missing parameters"),
        @ApiResponse(responseCode = "403", description = "Forbidden - insufficient privileges")
      })
  public ResponseEntity<ConsistencyCheckResult> check(
      HttpServletRequest request, @RequestBody @Nonnull ConsistencyCheckRequest checkRequest) {

    OperationContext opContext = getOperationContext(request, "check");
    try {
      SystemMetadataFilter filterConfig =
          buildFilterWithGracePeriod(
              checkRequest.getFilter(), checkRequest.getGracePeriodSeconds());

      CheckResult result =
          consistencyService.checkBatch(
              opContext,
              CheckBatchRequest.builder()
                  .entityType(checkRequest.getEntityType())
                  .checkIds(checkRequest.getCheckIds())
                  .batchSize(checkRequest.getBatchSize())
                  .scrollId(checkRequest.getScrollId())
                  .filter(filterConfig)
                  .build());

      return ResponseEntity.ok(ConsistencyCheckResult.from(result));
    } catch (IllegalArgumentException e) {
      log.warn("Invalid check request: {}", e.getMessage());
      return ResponseEntity.badRequest().build();
    } catch (Exception e) {
      log.error("Error running consistency checks: {}", e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  // ============================================================================
  // Fix Issues
  // ============================================================================

  @PostMapping(
      path = "/fix-issues",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Fix specific issues",
      description =
          "Applies fixes to the provided issues (typically from a /check response). "
              + "Default is dry-run mode which reports what would be fixed without making changes. "
              + "Writes are async by default for better performance.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Fix operation completed",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ConsistencyFixIssuesResult.class))),
        @ApiResponse(responseCode = "400", description = "Bad request - invalid issues"),
        @ApiResponse(responseCode = "403", description = "Forbidden - insufficient privileges")
      })
  public ResponseEntity<ConsistencyFixIssuesResult> fixIssues(
      HttpServletRequest request, @RequestBody @Nonnull ConsistencyFixIssuesRequest fixRequest) {

    OperationContext opContext = getOperationContext(request, "fixIssues");
    try {
      // Convert API issues to service-layer issues, discovering fix types when needed
      List<ConsistencyIssue> issues = new ArrayList<>();
      for (io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue apiIssue :
          fixRequest.getIssues()) {
        if (apiIssue.getFixType() == null && apiIssue.getCheckId() != null) {
          // No fixType provided but checkId is - run check to discover the actual issue
          Urn urn = UrnUtils.getUrn(apiIssue.getEntityUrn());
          java.util.Optional<ConsistencyIssue> discoveredIssue =
              consistencyService.discoverIssue(opContext, urn, apiIssue.getCheckId());
          if (discoveredIssue.isPresent()) {
            issues.add(discoveredIssue.get());
          } else {
            log.info(
                "No issue found for URN {} with check {}",
                apiIssue.getEntityUrn(),
                apiIssue.getCheckId());
          }
        } else if (apiIssue.getFixType() != null) {
          // fixType provided - convert directly
          issues.add(toServiceIssue(apiIssue));
        } else {
          throw new IllegalArgumentException(
              "Either fixType or checkId must be provided for issue with URN: "
                  + apiIssue.getEntityUrn());
        }
      }

      if (issues.isEmpty()) {
        // No issues to fix
        return ResponseEntity.ok(
            ConsistencyFixIssuesResult.from(
                ConsistencyFixResult.builder()
                    .dryRun(fixRequest.isDryRun())
                    .totalProcessed(0)
                    .entitiesFixed(0)
                    .entitiesFailed(0)
                    .fixDetails(List.of())
                    .build()));
      }

      ConsistencyFixResult result =
          consistencyService.fixIssues(opContext, issues, fixRequest.isDryRun());

      return ResponseEntity.ok(ConsistencyFixIssuesResult.from(result));
    } catch (IllegalArgumentException e) {
      log.warn("Invalid fix request: {}", e.getMessage());
      return ResponseEntity.badRequest().build();
    } catch (Exception e) {
      log.error("Error fixing issues: {}", e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  // ============================================================================
  // Check and Fix
  // ============================================================================

  @PostMapping(
      path = "/fix",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Check and fix in one operation",
      description =
          "Runs consistency checks on the specified entity type and automatically applies fixes to any issues found. "
              + "This is the primary endpoint for remediation. "
              + "Default is dry-run mode which reports what would be fixed without making changes. "
              + "Writes are async by default for better performance. "
              + "Use GET /consistency/checks to discover available entity types and check IDs.",
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Check and fix completed",
            content =
                @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema =
                        @Schema(
                            implementation =
                                io.datahubproject.openapi.operations.consistency.models
                                    .ConsistencyFixResult.class),
                    examples =
                        @ExampleObject(
                            value =
                                """
                    {
                      "entitiesScanned": 100,
                      "issuesFound": 3,
                      "scrollId": "eyJzb3J0IjpbMTcwMjk1....",
                      "dryRun": true,
                      "totalProcessed": 3,
                      "entitiesFixed": 2,
                      "entitiesFailed": 1,
                      "fixDetails": []
                    }
                    """))),
        @ApiResponse(
            responseCode = "400",
            description = "Bad request - invalid or missing parameters"),
        @ApiResponse(responseCode = "403", description = "Forbidden - insufficient privileges")
      })
  public ResponseEntity<
          io.datahubproject.openapi.operations.consistency.models.ConsistencyFixResult>
      fix(HttpServletRequest request, @RequestBody @Nonnull ConsistencyFixRequest fixRequest) {

    OperationContext opContext = getOperationContext(request, "fix");
    try {
      SystemMetadataFilter filterConfig =
          buildFilterWithGracePeriod(fixRequest.getFilter(), fixRequest.getGracePeriodSeconds());

      // Run checks
      CheckResult checkResult =
          consistencyService.checkBatch(
              opContext,
              CheckBatchRequest.builder()
                  .entityType(fixRequest.getEntityType())
                  .checkIds(fixRequest.getCheckIds())
                  .batchSize(fixRequest.getBatchSize())
                  .scrollId(fixRequest.getScrollId())
                  .filter(filterConfig)
                  .build());

      // Fix any issues found
      ConsistencyFixResult fixResult =
          consistencyService.fixIssues(opContext, checkResult.getIssues(), fixRequest.isDryRun());

      return ResponseEntity.ok(
          io.datahubproject.openapi.operations.consistency.models.ConsistencyFixResult.from(
              checkResult, fixResult));
    } catch (IllegalArgumentException e) {
      log.warn("Invalid fix request: {}", e.getMessage());
      return ResponseEntity.badRequest().build();
    } catch (Exception e) {
      log.error("Error running check and fix: {}", e.getMessage(), e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Convert an API ConsistencyIssue to a service-layer Issue.
   *
   * <p>Note: This conversion loses BatchItem information since the API model doesn't carry it. The
   * fix implementation will need to reconstruct the fix from the issue metadata.
   */
  private ConsistencyIssue toServiceIssue(
      io.datahubproject.openapi.operations.consistency.models.ConsistencyIssue apiIssue) {
    Urn urn = UrnUtils.getUrn(apiIssue.getEntityUrn());
    // Derive entityType from URN if not explicitly provided
    String entityType =
        apiIssue.getEntityType() != null ? apiIssue.getEntityType() : urn.getEntityType();

    return ConsistencyIssue.builder()
        .entityUrn(urn)
        .entityType(entityType)
        .checkId(apiIssue.getCheckId())
        .fixType(apiIssue.getFixType())
        .description(apiIssue.getDescription())
        .relatedUrns(
            apiIssue.getRelatedUrns() != null
                ? apiIssue.getRelatedUrns().stream()
                    .map(UrnUtils::getUrn)
                    .collect(Collectors.toList())
                : null)
        .details(apiIssue.getDetails())
        .build();
  }

  /**
   * Creates an operation context for the current request after verifying authorization.
   *
   * @param request the HTTP request
   * @param operationName the name of the operation for logging
   * @return the operation context (never null)
   * @throws UnauthorizedException if the user lacks required privileges
   */
  private OperationContext getOperationContext(HttpServletRequest request, String operationName) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildOpenapi(actorUrnStr, request, operationName, List.of()),
            authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIOperationsAuthorized(
        opContext, PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)) {
      throw new UnauthorizedException(
          actorUrnStr + " is not authorized for consistency operations");
    }

    return opContext;
  }

  /**
   * Build a filter with grace period applied.
   *
   * <p>The grace period excludes entities modified within the specified window to avoid false
   * positives from eventual consistency race conditions.
   *
   * @param requestFilter the filter from the request (may be null)
   * @param requestGracePeriodSeconds grace period from request (overrides default if not null)
   * @return filter with lePitEpochMs set if grace period applies
   */
  @Nullable
  private SystemMetadataFilter buildFilterWithGracePeriod(
      @Nullable
          io.datahubproject.openapi.operations.consistency.models.SystemMetadataFilter
              requestFilter,
      @Nullable Long requestGracePeriodSeconds) {

    // Determine effective grace period: request param overrides config default
    long effectiveGracePeriodSeconds =
        requestGracePeriodSeconds != null
            ? requestGracePeriodSeconds
            : consistencyChecksConfig.getGracePeriodSeconds();

    // If grace period is 0 or negative, don't apply it
    if (effectiveGracePeriodSeconds <= 0) {
      return requestFilter != null ? requestFilter.toServiceFilter() : null;
    }

    // Check if lePitEpochMs is already explicitly set
    if (requestFilter != null && requestFilter.getLePitEpochMs() != null) {
      // User explicitly set lePitEpochMs, honor their setting
      return requestFilter.toServiceFilter();
    }

    // Compute lePitEpochMs from grace period
    long lePitEpochMs = System.currentTimeMillis() - (effectiveGracePeriodSeconds * 1000L);

    // Build the filter with grace period applied
    if (requestFilter != null) {
      // Merge with existing filter
      return SystemMetadataFilter.builder()
          .gePitEpochMs(requestFilter.getGePitEpochMs())
          .lePitEpochMs(lePitEpochMs)
          .aspectFilters(requestFilter.getAspectFilters())
          .includeSoftDeleted(requestFilter.isIncludeSoftDeleted())
          .build();
    } else {
      // Create new filter with just the grace period
      return SystemMetadataFilter.builder().lePitEpochMs(lePitEpochMs).build();
    }
  }
}
