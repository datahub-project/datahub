package com.linkedin.metadata.aspect.consistency;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixDetail;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.utils.UrnExtractionUtils;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;

/**
 * Generic service for checking and fixing consistency issues across entity types.
 *
 * <p>This service processes <b>single batches</b> only - no internal loops. Callers (API endpoints,
 * upgrade jobs) are responsible for:
 *
 * <ul>
 *   <li>Looping through batches
 *   <li>Managing batch size and delays
 *   <li>Handling pagination via scrollId
 * </ul>
 *
 * <p>Key methods:
 *
 * <ul>
 *   <li>{@link #checkBatch(OperationContext, String, List, int, String, SystemMetadataFilterConfig,
 *       CheckContext)} - Run checks for a single entity type
 *   <li>{@link #fixIssues} - Apply fixes to a list of issues
 * </ul>
 *
 * <p><b>Fetch Strategy:</b> Uses the system metadata Elasticsearch index which stores (URN, aspect)
 * pairs. This enables filtering by entity type (via URN prefix), aspect existence, and timestamps
 * (aspectModifiedTime/aspectCreatedTime). Entity data is then fetched from SQL via EntityService.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Each method call creates its own {@link
 * CheckContext} which is not shared between threads.
 */
@Slf4j
@ThreadSafe
public class ConsistencyService {

  private final EntityService<?> entityService;
  private final ESSystemMetadataDAO esSystemMetadataDAO;
  @Nullable private final GraphClient graphClient;
  private final ConsistencyCheckRegistry checkRegistry;
  private final ConsistencyFixRegistry fixRegistry;

  /** Per-check configuration (check ID -> config key -> value) */
  @Nonnull private final Map<String, Map<String, String>> checkConfigs;

  /**
   * Create a ConsistencyService without check-specific configuration.
   *
   * @param entityService entity service for fetching entity data from SQL
   * @param esSystemMetadataDAO system metadata DAO for querying the system metadata index
   * @param graphClient graph client (may be null)
   * @param checkRegistry registry of consistency checks
   * @param fixRegistry registry of consistency fixes
   */
  public ConsistencyService(
      @Nonnull EntityService<?> entityService,
      @Nonnull ESSystemMetadataDAO esSystemMetadataDAO,
      @Nullable GraphClient graphClient,
      @Nonnull ConsistencyCheckRegistry checkRegistry,
      @Nonnull ConsistencyFixRegistry fixRegistry) {
    this(entityService, esSystemMetadataDAO, graphClient, checkRegistry, fixRegistry, Map.of());
  }

  /**
   * Create a ConsistencyService with full configuration.
   *
   * @param entityService entity service for fetching entity data from SQL
   * @param esSystemMetadataDAO system metadata DAO for querying the system metadata index
   * @param graphClient graph client (may be null)
   * @param checkRegistry registry of consistency checks
   * @param fixRegistry registry of consistency fixes
   * @param checkConfigs per-check configuration from consistencyChecks.checks in application.yaml
   */
  public ConsistencyService(
      @Nonnull EntityService<?> entityService,
      @Nonnull ESSystemMetadataDAO esSystemMetadataDAO,
      @Nullable GraphClient graphClient,
      @Nonnull ConsistencyCheckRegistry checkRegistry,
      @Nonnull ConsistencyFixRegistry fixRegistry,
      @Nonnull Map<String, Map<String, String>> checkConfigs) {
    this.entityService = entityService;
    this.esSystemMetadataDAO = esSystemMetadataDAO;
    this.graphClient = graphClient;
    this.checkRegistry = checkRegistry;
    this.fixRegistry = fixRegistry;
    this.checkConfigs = checkConfigs;

    log.info(
        "ConsistencyService initialized with {} checks, {} fixes, {} check configs",
        checkRegistry.size(),
        fixRegistry.size(),
        checkConfigs.size());
  }

  /**
   * Get the check registry.
   *
   * @return the check registry
   */
  public ConsistencyCheckRegistry getCheckRegistry() {
    return checkRegistry;
  }

  /**
   * Get the fix registry.
   *
   * @return the fix registry
   */
  public ConsistencyFixRegistry getFixRegistry() {
    return fixRegistry;
  }

  // ============================================================================
  // Internal Check Operations - Single Batch (package-private for testing)
  // ============================================================================

  /**
   * Run checks on a pre-fetched batch of entity responses.
   *
   * <p>This is an internal method - external callers should use {@link
   * #checkBatch(OperationContext, List, int, Map, CheckContext)} which handles entity fetching.
   *
   * @param opContext operation context
   * @param entityType entity type (e.g., "assertion", "monitor")
   * @param entityResponses map of URN to entity response
   * @return list of issues found
   */
  List<ConsistencyIssue> checkBatchInternal(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Map<Urn, EntityResponse> entityResponses) {
    return checkBatchInternal(opContext, entityType, entityResponses, null);
  }

  /**
   * Run checks on a pre-fetched batch of entity responses with a shared context.
   *
   * <p>Package-private for internal use and testing.
   *
   * @param opContext operation context
   * @param entityType entity type (e.g., "assertion", "monitor")
   * @param entityResponses map of URN to entity response
   * @param existingCtx optional existing context (e.g., with pre-populated soft-deleted entities)
   * @return list of issues found
   */
  List<ConsistencyIssue> checkBatchInternal(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Map<Urn, EntityResponse> entityResponses,
      @Nullable CheckContext existingCtx) {
    return checkBatchInternal(opContext, entityType, entityResponses, existingCtx, null);
  }

  /**
   * Run checks on a pre-fetched batch of entity responses with filtering by check IDs.
   *
   * <p>Package-private for internal use and testing.
   *
   * <p>When checkIds is null or empty (wildcard), only default checks are run (excludes on-demand
   * only checks). When checkIds is explicitly specified, those checks are run even if on-demand.
   *
   * @param opContext operation context
   * @param entityType entity type (e.g., "assertion", "monitor")
   * @param entityResponses map of URN to entity response
   * @param existingCtx optional existing context (e.g., with pre-populated soft-deleted entities)
   * @param checkIds optional list of check IDs to run (if null or empty, runs default checks only)
   * @return list of issues found
   */
  List<ConsistencyIssue> checkBatchInternal(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Map<Urn, EntityResponse> entityResponses,
      @Nullable CheckContext existingCtx,
      @Nullable List<String> checkIds) {

    if (entityResponses.isEmpty()) {
      return List.of();
    }

    List<ConsistencyIssue> allIssues = new ArrayList<>();
    CheckContext ctx = existingCtx != null ? existingCtx : buildContext(opContext);
    // Use getDefaultByEntityTypeAndIds which excludes on-demand checks unless explicitly requested
    List<ConsistencyCheck> checks =
        checkRegistry.getDefaultByEntityTypeAndIds(entityType, checkIds);

    for (ConsistencyCheck check : checks) {
      try {
        List<ConsistencyIssue> issues = check.check(ctx, entityResponses);
        allIssues.addAll(issues);
      } catch (Exception e) {
        log.error(
            "Error running check {} on {} entities: {}. Continuing with other checks.",
            check.getId(),
            entityResponses.size(),
            e.getMessage(),
            e);
      }
    }

    return allIssues;
  }

  // ============================================================================
  // Public Check API
  // ============================================================================

  /**
   * Run consistency checks on entities using a request object.
   *
   * <p>This is the preferred API for running batch consistency checks. Use the builder to construct
   * requests with sensible defaults.
   *
   * <p>Example:
   *
   * <pre>{@code
   * CheckResult result = service.checkBatch(opContext,
   *     CheckBatchRequest.builder()
   *         .entityType("assertion")
   *         .batchSize(100)
   *         .build());
   * }</pre>
   *
   * @param opContext operation context
   * @param request the check batch request configuration
   * @return check result with issues and scrollId for continuation
   * @throws IllegalArgumentException if checkIds span multiple entity types or if neither
   *     entityType nor checkIds is provided
   */
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext, @Nonnull CheckBatchRequest request) {
    return checkBatch(opContext, request, null);
  }

  /**
   * Run consistency checks with a shared context for cross-batch state.
   *
   * <p>Use this overload when processing multiple batches and need to share state (like cached
   * aspects) across batches for efficiency.
   *
   * @param opContext operation context
   * @param request the check batch request configuration
   * @param existingCtx optional existing context for cross-batch state
   * @return check result with issues and scrollId for continuation
   * @throws IllegalArgumentException if checkIds span multiple entity types or if neither
   *     entityType nor checkIds is provided
   */
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext,
      @Nonnull CheckBatchRequest request,
      @Nullable CheckContext existingCtx) {

    // Resolve entity type - from request, URNs, or checkIds
    String resolvedEntityType = resolveEntityType(request);

    // Validate URNs match entity type if both provided
    if (request.getUrns() != null && !request.getUrns().isEmpty()) {
      for (Urn urn : request.getUrns()) {
        if (!urn.getEntityType().equals(resolvedEntityType)) {
          throw new IllegalArgumentException(
              String.format(
                  "URN %s has entity type '%s' but entityType '%s' was resolved",
                  urn, urn.getEntityType(), resolvedEntityType));
        }
      }
    }

    // Get checks for this entity type
    List<ConsistencyCheck> checks =
        checkRegistry.getDefaultByEntityTypeAndIds(resolvedEntityType, request.getCheckIds());

    if (checks.isEmpty()) {
      log.debug("No checks to run for entity type {}", resolvedEntityType);
      return CheckResult.empty();
    }

    // Derive required aspects from checks (consolidated)
    // Returns null if any check requires all aspects
    Set<String> requiredAspects = deriveRequiredAspects(checks);

    // If requiredAspects is null, get all aspect names for the entity type
    if (requiredAspects == null) {
      requiredAspects =
          opContext
              .getEntityRegistry()
              .getEntitySpec(resolvedEntityType)
              .getAspectSpecMap()
              .keySet();
    }

    // Build query for system metadata index (URNs are just another filter)
    BoolQueryBuilder query =
        buildSystemMetadataQuery(
            resolvedEntityType, checks, request.getFilter(), request.getUrns());

    // Scroll system metadata index
    SearchResponse response =
        esSystemMetadataDAO.scroll(
            query,
            request.isIncludeSoftDeleted(),
            request.getScrollId(),
            null, // pitId
            "5m", // keepAlive
            request.getBatchSize());

    if (response == null
        || response.getHits() == null
        || response.getHits().getHits().length == 0) {
      return CheckResult.empty();
    }

    // Extract unique URNs from results
    Set<Urn> urnsFromEs = UrnExtractionUtils.extractUniqueUrns(response);

    if (urnsFromEs.isEmpty()) {
      return CheckResult.empty();
    }

    // Fetch entity data via EntityService (from SQL)
    Map<Urn, EntityResponse> entityResponses;
    try {
      entityResponses =
          entityService.getEntitiesV2(
              opContext, resolvedEntityType, urnsFromEs, requiredAspects, false);
    } catch (java.net.URISyntaxException e) {
      log.error("Failed to fetch entities for type {}: {}", resolvedEntityType, e.getMessage(), e);
      return CheckResult.empty();
    }

    // Identify orphan URNs - entities in ES index but not in SQL (uses key aspect check)
    Set<Urn> orphanUrns = identifyOrphanUrns(opContext, urnsFromEs);

    // Run checks
    CheckContext ctx = existingCtx != null ? existingCtx : buildContext(opContext);

    // Store orphan URNs in context for the orphan check
    if (!orphanUrns.isEmpty()) {
      ctx.addOrphanUrns(resolvedEntityType, orphanUrns);
      log.info(
          "Found {} orphan URNs for entity type {} (exist in ES but not SQL)",
          orphanUrns.size(),
          resolvedEntityType);
    }

    List<ConsistencyIssue> issues = runChecks(ctx, entityResponses, checks);

    // Run orphan check if there are orphan URNs
    if (!orphanUrns.isEmpty()) {
      List<ConsistencyIssue> orphanIssues = runOrphanCheck(ctx, resolvedEntityType);
      issues.addAll(orphanIssues);
    }

    // Extract next scrollId from search response
    String nextScrollId = extractNextScrollId(response);

    return CheckResult.builder()
        .entitiesScanned(urnsFromEs.size())
        .issuesFound(issues.size())
        .issues(issues)
        .scrollId(nextScrollId)
        .build();
  }

  /**
   * Resolve entity type from request parameters.
   *
   * <p>Priority: 1) explicit entityType, 2) derived from URNs, 3) derived from checkIds
   */
  private String resolveEntityType(CheckBatchRequest request) {
    // Try to derive from URNs first (URNs are most explicit)
    if (request.getUrns() != null && !request.getUrns().isEmpty()) {
      Set<String> entityTypes =
          request.getUrns().stream()
              .map(Urn::getEntityType)
              .collect(java.util.stream.Collectors.toSet());
      if (entityTypes.size() > 1) {
        throw new IllegalArgumentException(
            "All URNs must be of the same entity type. Found: " + entityTypes);
      }
      String urnEntityType = entityTypes.iterator().next();

      // Validate entityType matches URNs if both provided
      if (request.getEntityType() != null
          && !request.getEntityType().isEmpty()
          && !request.getEntityType().equals(urnEntityType)) {
        throw new IllegalArgumentException(
            String.format(
                "entityType '%s' does not match URN entity type '%s'",
                request.getEntityType(), urnEntityType));
      }
      return urnEntityType;
    }

    // Use resolveAndValidateEntityType for entityType + checkIds validation
    return resolveAndValidateEntityType(request.getEntityType(), request.getCheckIds());
  }

  // ============================================================================
  // Deprecated Check API (use CheckBatchRequest instead)
  // ============================================================================

  /**
   * Run consistency checks on entities using the default fetch strategy.
   *
   * @deprecated Use {@link #checkBatch(OperationContext, CheckBatchRequest)} instead.
   */
  @Deprecated
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext,
      @Nullable String entityType,
      @Nullable List<String> checkIds,
      int batchSize) {
    return checkBatch(
        opContext,
        CheckBatchRequest.builder()
            .entityType(entityType)
            .checkIds(checkIds)
            .batchSize(batchSize)
            .build());
  }

  /**
   * Run consistency checks with pagination continuation.
   *
   * @deprecated Use {@link #checkBatch(OperationContext, CheckBatchRequest)} instead.
   */
  @Deprecated
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext,
      @Nullable String entityType,
      @Nullable List<String> checkIds,
      int batchSize,
      @Nullable String scrollId) {
    return checkBatch(
        opContext,
        CheckBatchRequest.builder()
            .entityType(entityType)
            .checkIds(checkIds)
            .batchSize(batchSize)
            .scrollId(scrollId)
            .build());
  }

  /**
   * Run consistency checks with pagination and shared context.
   *
   * @deprecated Use {@link #checkBatch(OperationContext, CheckBatchRequest, CheckContext)} instead.
   */
  @Deprecated
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext,
      @Nullable String entityType,
      @Nullable List<String> checkIds,
      int batchSize,
      @Nullable String scrollId,
      @Nullable CheckContext existingCtx) {
    return checkBatch(
        opContext,
        CheckBatchRequest.builder()
            .entityType(entityType)
            .checkIds(checkIds)
            .batchSize(batchSize)
            .scrollId(scrollId)
            .build(),
        existingCtx);
  }

  /**
   * Run consistency checks with full configuration.
   *
   * @deprecated Use {@link #checkBatch(OperationContext, CheckBatchRequest, CheckContext)} instead.
   */
  @Deprecated
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext,
      @Nullable String entityType,
      @Nullable List<String> checkIds,
      int batchSize,
      @Nullable String scrollId,
      @Nullable SystemMetadataFilter filter,
      @Nullable CheckContext existingCtx) {
    return checkBatch(
        opContext,
        CheckBatchRequest.builder()
            .entityType(entityType)
            .checkIds(checkIds)
            .batchSize(batchSize)
            .scrollId(scrollId)
            .filter(filter)
            .build(),
        existingCtx);
  }

  /**
   * Run consistency checks with full configuration including soft-delete control.
   *
   * @deprecated Use {@link #checkBatch(OperationContext, CheckBatchRequest, CheckContext)} instead.
   *     Set includeSoftDeleted via {@link SystemMetadataFilter#isIncludeSoftDeleted()}.
   */
  @Deprecated
  public CheckResult checkBatch(
      @Nonnull OperationContext opContext,
      @Nullable String entityType,
      @Nullable List<String> checkIds,
      int batchSize,
      @Nullable String scrollId,
      @Nullable SystemMetadataFilter filter,
      boolean includeSoftDeleted,
      @Nullable CheckContext existingCtx) {
    // Build filter with includeSoftDeleted if needed
    SystemMetadataFilter effectiveFilter = filter;
    if (includeSoftDeleted && (filter == null || !filter.isIncludeSoftDeleted())) {
      effectiveFilter =
          filter != null
              ? SystemMetadataFilter.builder()
                  .gePitEpochMs(filter.getGePitEpochMs())
                  .lePitEpochMs(filter.getLePitEpochMs())
                  .aspectFilters(filter.getAspectFilters())
                  .includeSoftDeleted(true)
                  .build()
              : SystemMetadataFilter.builder().includeSoftDeleted(true).build();
    }
    return checkBatch(
        opContext,
        CheckBatchRequest.builder()
            .entityType(entityType)
            .checkIds(checkIds)
            .batchSize(batchSize)
            .scrollId(scrollId)
            .filter(effectiveFilter)
            .build(),
        existingCtx);
  }

  /**
   * Process batch results after data has been fetched from ES and SQL.
   *
   * <p>This method handles orphan detection, running checks, and building the result. It is
   * separated from data fetching to enable easier unit testing.
   *
   * <p>Package-private for testing.
   *
   * @param opContext operation context
   * @param entityType resolved entity type
   * @param urnsFromEs URNs extracted from ES system metadata index
   * @param entitiesFromSql entity responses fetched from SQL via EntityService
   * @param checks consistency checks to run
   * @param existingCtx optional existing check context (for reuse across batches)
   * @param nextScrollId scroll ID for pagination (from ES response)
   * @return check result with issues found
   */
  CheckResult processBatchResults(
      @Nullable OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Set<Urn> urnsFromEs,
      @Nonnull Map<Urn, EntityResponse> entitiesFromSql,
      @Nonnull List<ConsistencyCheck> checks,
      @Nullable CheckContext existingCtx,
      @Nullable String nextScrollId) {

    // Identify orphan URNs - entities in ES index but not in SQL (uses key aspect check)
    Set<Urn> orphanUrns = identifyOrphanUrns(opContext, urnsFromEs);

    // Run checks
    CheckContext ctx = existingCtx != null ? existingCtx : buildContext(opContext);

    // Store orphan URNs in context for the orphan check
    if (!orphanUrns.isEmpty()) {
      ctx.addOrphanUrns(entityType, orphanUrns);
      log.info(
          "Found {} orphan URNs for entity type {} (exist in ES but not SQL)",
          orphanUrns.size(),
          entityType);
    }

    List<ConsistencyIssue> issues = runChecks(ctx, entitiesFromSql, checks);

    // Run orphan check if there are orphan URNs
    if (!orphanUrns.isEmpty()) {
      List<ConsistencyIssue> orphanIssues = runOrphanCheck(ctx, entityType);
      issues.addAll(orphanIssues);
    }

    return CheckResult.builder()
        .entitiesScanned(urnsFromEs.size()) // Count all URNs scanned, including orphans
        .issuesFound(issues.size())
        .issues(issues)
        .scrollId(nextScrollId)
        .build();
  }

  // ============================================================================
  // System Metadata Query Building
  // ============================================================================

  /**
   * Build a BoolQueryBuilder for the system metadata index.
   *
   * <p>Package-private for testing.
   *
   * @param entityType entity type
   * @param checks checks to run
   * @param filter optional filter configuration
   * @param urns optional URN filter - when provided, limits query to these specific URNs
   * @return BoolQueryBuilder for the system metadata index
   */
  BoolQueryBuilder buildSystemMetadataQuery(
      @Nonnull String entityType,
      @Nonnull List<ConsistencyCheck> checks,
      @Nullable SystemMetadataFilter filter,
      @Nullable Set<Urn> urns) {

    BoolQueryBuilder query = QueryBuilders.boolQuery();

    // Filter by entity type via URN prefix
    query.filter(QueryBuilders.prefixQuery("urn", "urn:li:" + entityType + ":"));

    // Filter by specific URNs if provided
    if (urns != null && !urns.isEmpty()) {
      Set<String> urnStrings =
          urns.stream().map(Urn::toString).collect(java.util.stream.Collectors.toSet());
      query.filter(QueryBuilders.termsQuery("urn", urnStrings));
    }

    // Filter by aspect if check or config requires it
    Set<String> targetAspects = getTargetAspects(checks, filter);
    if (!targetAspects.isEmpty()) {
      if (targetAspects.size() == 1) {
        query.filter(QueryBuilders.termQuery("aspect", targetAspects.iterator().next()));
      } else {
        query.filter(QueryBuilders.termsQuery("aspect", targetAspects));
      }
    }

    // Timestamp filters - use aspectModifiedTime (preferred) with aspectCreatedTime fallback
    if (filter != null && (filter.getGePitEpochMs() != null || filter.getLePitEpochMs() != null)) {
      BoolQueryBuilder timestampQuery = QueryBuilders.boolQuery();

      // Try aspectModifiedTime first
      BoolQueryBuilder modifiedTimeQuery = QueryBuilders.boolQuery();
      modifiedTimeQuery.must(QueryBuilders.existsQuery("aspectModifiedTime"));
      if (filter.getGePitEpochMs() != null) {
        modifiedTimeQuery.must(
            QueryBuilders.rangeQuery("aspectModifiedTime").gte(filter.getGePitEpochMs()));
      }
      if (filter.getLePitEpochMs() != null) {
        modifiedTimeQuery.must(
            QueryBuilders.rangeQuery("aspectModifiedTime").lte(filter.getLePitEpochMs()));
      }

      // Fallback to aspectCreatedTime if aspectModifiedTime doesn't exist
      BoolQueryBuilder createdTimeQuery = QueryBuilders.boolQuery();
      createdTimeQuery.mustNot(QueryBuilders.existsQuery("aspectModifiedTime"));
      if (filter.getGePitEpochMs() != null) {
        createdTimeQuery.must(
            QueryBuilders.rangeQuery("aspectCreatedTime").gte(filter.getGePitEpochMs()));
      }
      if (filter.getLePitEpochMs() != null) {
        createdTimeQuery.must(
            QueryBuilders.rangeQuery("aspectCreatedTime").lte(filter.getLePitEpochMs()));
      }

      timestampQuery.should(modifiedTimeQuery);
      timestampQuery.should(createdTimeQuery);
      timestampQuery.minimumShouldMatch(1);

      query.filter(timestampQuery);
    }

    return query;
  }

  /**
   * Get target aspects from checks or filter config.
   *
   * @param checks list of checks
   * @param filter optional filter configuration
   * @return set of target aspect names (may be empty)
   */
  @Nonnull
  private Set<String> getTargetAspects(
      @Nonnull List<ConsistencyCheck> checks, @Nullable SystemMetadataFilter filter) {
    Set<String> aspects = new HashSet<>();

    // Add from filter config first (now supports list)
    if (filter != null
        && filter.getAspectFilters() != null
        && !filter.getAspectFilters().isEmpty()) {
      aspects.addAll(filter.getAspectFilters());
    }

    // Add target aspects from all checks
    for (ConsistencyCheck check : checks) {
      aspects.addAll(check.getTargetAspects());
    }

    return aspects;
  }

  /**
   * Extract next scroll ID from search response for pagination.
   *
   * <p>Package-private for testing.
   *
   * @param response search response
   * @return scroll ID or null if no more results
   */
  @Nullable
  String extractNextScrollId(@Nonnull SearchResponse response) {
    SearchHit[] hits = response.getHits().getHits();
    if (hits.length == 0) {
      return null;
    }

    // Get sort values from last hit for search_after pagination
    SearchHit lastHit = hits[hits.length - 1];
    Object[] sortValues = lastHit.getSortValues();
    if (sortValues == null || sortValues.length == 0) {
      return null;
    }

    // Create SearchAfterWrapper and encode as scrollId
    SearchAfterWrapper wrapper = new SearchAfterWrapper(sortValues, null, 0);
    return wrapper.toScrollId();
  }

  /**
   * Resolve entity type from parameters and validate checkIds are for a single entity type.
   *
   * @param entityType explicit entity type (may be null)
   * @param checkIds check IDs (may be null/empty)
   * @return resolved entity type
   * @throws IllegalArgumentException if validation fails
   */
  private String resolveAndValidateEntityType(
      @Nullable String entityType, @Nullable List<String> checkIds) {

    // If checkIds provided, validate they're all for the same entity type
    if (checkIds != null && !checkIds.isEmpty()) {
      Set<String> entityTypes = new HashSet<>();
      for (String checkId : checkIds) {
        checkRegistry.getById(checkId).ifPresent(check -> entityTypes.add(check.getEntityType()));
      }

      if (entityTypes.isEmpty()) {
        throw new IllegalArgumentException("No valid checks found for IDs: " + checkIds);
      }

      // Remove wildcard from entity types - wildcard checks apply to any entity type
      entityTypes.remove(ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE);

      // If only wildcard checks are specified, entityType is required
      if (entityTypes.isEmpty()) {
        if (entityType == null || entityType.isEmpty()) {
          throw new IllegalArgumentException(
              "entityType is required when only wildcard checks are specified. "
                  + "Wildcard checks apply to all entity types.");
        }
        return entityType;
      }

      if (entityTypes.size() > 1) {
        throw new IllegalArgumentException(
            "All checkIds must be for the same entity type. Found: " + entityTypes);
      }

      String derivedEntityType = entityTypes.iterator().next();

      // If entityType also provided, validate it matches
      if (entityType != null && !entityType.equals(derivedEntityType)) {
        throw new IllegalArgumentException(
            String.format(
                "entityType '%s' does not match entity type '%s' from checkIds",
                entityType, derivedEntityType));
      }

      return derivedEntityType;
    }

    // No checkIds - entityType is required
    if (entityType == null || entityType.isEmpty()) {
      throw new IllegalArgumentException("Either entityType or checkIds must be provided");
    }

    return entityType;
  }

  /**
   * Derive required aspects from checks.
   *
   * <p>Consolidates aspects from all checks - if multiple checks need the same aspects, they are
   * only fetched once. Always includes STATUS_ASPECT_NAME for soft-delete tracking.
   *
   * <p>If any check requires all aspects (returns true for {@link
   * ConsistencyCheck#requiresAllAspects()}), returns null to indicate all aspects should be
   * fetched.
   *
   * @param checks list of checks
   * @return aggregated set of required aspects (union of all checks), or null if all aspects needed
   */
  @Nullable
  public Set<String> deriveRequiredAspects(List<ConsistencyCheck> checks) {
    Set<String> aspects = new HashSet<>();
    aspects.add(STATUS_ASPECT_NAME); // Always needed for soft-delete tracking

    for (ConsistencyCheck check : checks) {
      java.util.Optional<Set<String>> required = check.getRequiredAspects();
      if (required.isEmpty()) {
        // Empty optional = check requires all aspects
        return null;
      }
      aspects.addAll(required.get());
    }
    return aspects;
  }

  /**
   * Run checks on pre-fetched entity responses.
   *
   * @param ctx check context
   * @param entityResponses map of URN to entity response
   * @param checks checks to run
   * @return list of issues found
   */
  private List<ConsistencyIssue> runChecks(
      CheckContext ctx, Map<Urn, EntityResponse> entityResponses, List<ConsistencyCheck> checks) {
    List<ConsistencyIssue> allIssues = new ArrayList<>();
    for (ConsistencyCheck check : checks) {
      try {
        List<ConsistencyIssue> issues = check.check(ctx, entityResponses);
        allIssues.addAll(issues);
      } catch (Exception e) {
        log.error(
            "Error running check {} on {} entities: {}. Continuing with other checks.",
            check.getId(),
            entityResponses.size(),
            e.getMessage(),
            e);
      }
    }
    return allIssues;
  }

  /**
   * Identify orphan URNs - URNs found in ES index but not in SQL.
   *
   * <p>This method uses {@link EntityService#exists} with aspectName=null, which checks for the
   * entity's KEY ASPECT. This is the correct way to determine if an entity exists in SQL, as the
   * key aspect is the fundamental aspect that always exists for any entity.
   *
   * <p>Package-private for testing.
   *
   * @param opContext operation context for entity service calls
   * @param urnsFromEs URNs extracted from ES system metadata index
   * @return set of orphan URNs (in ES but not in SQL)
   */
  Set<Urn> identifyOrphanUrns(OperationContext opContext, Set<Urn> urnsFromEs) {
    if (urnsFromEs.isEmpty()) {
      return Set.of();
    }

    // Use exists() with aspectName=null to check for the KEY aspect
    // This is the proper way to determine if an entity exists in SQL
    Set<Urn> existingUrns =
        entityService.exists(
            opContext,
            urnsFromEs,
            null, // null aspectName = check key aspect
            true, // includeSoftDeleted - we want to know if entity exists at all
            false); // forUpdate

    Set<Urn> orphans = new HashSet<>(urnsFromEs);
    orphans.removeAll(existingUrns);
    return orphans;
  }

  /**
   * Run the orphan index document check.
   *
   * @param ctx check context with orphan URNs populated
   * @param entityType entity type to check for orphans
   * @return list of issues for orphaned documents
   */
  private List<ConsistencyIssue> runOrphanCheck(CheckContext ctx, String entityType) {
    // Get the OrphanIndexDocumentCheck from the registry
    return checkRegistry
        .getById("orphan-index-document")
        .map(
            check -> {
              if (check
                  instanceof
                  com.linkedin.metadata.aspect.consistency.check.OrphanIndexDocumentCheck) {
                return ((com.linkedin.metadata.aspect.consistency.check.OrphanIndexDocumentCheck)
                        check)
                    .checkOrphans(ctx, entityType);
              }
              log.warn(
                  "OrphanIndexDocumentCheck found but is unexpected type: {}", check.getClass());
              return List.<ConsistencyIssue>of();
            })
        .orElseGet(
            () -> {
              log.debug("OrphanIndexDocumentCheck not registered, skipping orphan detection");
              return List.of();
            });
  }

  // ============================================================================
  // Fix Operations
  // ============================================================================

  /**
   * Apply fixes to a list of issues.
   *
   * <p>Each issue is processed independently. Failures on one issue don't stop processing of
   * others.
   *
   * @param opContext operation context
   * @param issues issues to fix
   * @param dryRun if true, only report what would be fixed without making changes
   * @return fix result with details of each fix operation
   */
  public ConsistencyFixResult fixIssues(
      @Nonnull OperationContext opContext, @Nonnull List<ConsistencyIssue> issues, boolean dryRun) {

    List<ConsistencyFixDetail> fixDetails = new ArrayList<>();
    int entitiesFixed = 0;
    int entitiesFailed = 0;

    for (ConsistencyIssue issue : issues) {
      try {
        ConsistencyFixDetail detail = applyFix(opContext, issue, dryRun);
        fixDetails.add(detail);

        if (detail.isSuccess()) {
          entitiesFixed++;
        } else {
          entitiesFailed++;
        }
      } catch (Exception e) {
        log.error(
            "Unexpected error fixing issue for {}: {}. Continuing with other issues.",
            issue.getEntityUrn(),
            e.getMessage(),
            e);
        fixDetails.add(
            ConsistencyFixDetail.builder()
                .urn(issue.getEntityUrn())
                .action(issue.getFixType())
                .success(false)
                .errorMessage("Unexpected error: " + e.getMessage())
                .build());
        entitiesFailed++;
      }
    }

    return ConsistencyFixResult.builder()
        .dryRun(dryRun)
        .totalProcessed(issues.size())
        .entitiesFixed(entitiesFixed)
        .entitiesFailed(entitiesFailed)
        .fixDetails(fixDetails)
        .build();
  }

  /**
   * Discover the actual issue for a URN by running the specified check.
   *
   * <p>Used when the API receives a minimal issue with just entityUrn and checkId, without a
   * fixType. The check is run against the entity to determine the actual issue and fix type.
   *
   * @param opContext operation context
   * @param urn the entity URN
   * @param checkId the check ID to run
   * @return the discovered issue, or empty if no issue found
   */
  public java.util.Optional<ConsistencyIssue> discoverIssue(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String checkId) {

    // Look up the check
    ConsistencyCheck check =
        checkRegistry
            .getById(checkId)
            .orElseThrow(() -> new IllegalArgumentException("Unknown check ID: " + checkId));

    String entityType = urn.getEntityType();

    // Derive required aspects
    Set<String> requiredAspects = deriveRequiredAspects(List.of(check));

    // If requiredAspects is null, get all aspect names
    if (requiredAspects == null) {
      requiredAspects =
          opContext.getEntityRegistry().getEntitySpec(entityType).getAspectSpecMap().keySet();
    }

    // For orphan check, we need to check if entity exists in SQL
    if ("orphan-index-document".equals(checkId)) {
      Set<Urn> existingUrns = entityService.exists(opContext, Set.of(urn), null, true, false);
      if (existingUrns.isEmpty()) {
        // Entity doesn't exist in SQL - create orphan issue
        return java.util.Optional.of(
            ConsistencyIssue.builder()
                .entityUrn(urn)
                .entityType(entityType)
                .checkId(checkId)
                .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
                .description(
                    String.format("Entity exists in ES indices but not in SQL (orphaned): %s", urn))
                .hardDeleteUrns(List.of(urn))
                .build());
      }
      // Entity exists - no orphan issue
      return java.util.Optional.empty();
    }

    // Fetch entity data
    Map<Urn, EntityResponse> entityResponses;
    try {
      entityResponses =
          entityService.getEntitiesV2(opContext, entityType, Set.of(urn), requiredAspects, false);
    } catch (java.net.URISyntaxException e) {
      log.error("Failed to fetch entity {}: {}", urn, e.getMessage(), e);
      return java.util.Optional.empty();
    }

    if (entityResponses.isEmpty()) {
      log.debug("Entity {} not found", urn);
      return java.util.Optional.empty();
    }

    // Run the check
    CheckContext ctx = buildContext(opContext);
    List<ConsistencyIssue> issues = check.check(ctx, entityResponses);

    // Return the first issue for this URN (there should be at most one per check)
    return issues.stream().filter(i -> i.getEntityUrn().equals(urn)).findFirst();
  }

  /**
   * Check a batch and fix any issues found.
   *
   * <p>Convenience method that combines checkBatch and fixIssues.
   *
   * @param opContext operation context
   * @param entityType entity type
   * @param entityResponses entity responses to check
   * @param dryRun if true, only report what would be fixed
   * @return fix result
   */
  public ConsistencyFixResult checkAndFixBatch(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull Map<Urn, EntityResponse> entityResponses,
      boolean dryRun) {

    List<ConsistencyIssue> issues = checkBatchInternal(opContext, entityType, entityResponses);
    return fixIssues(opContext, issues, dryRun);
  }

  // ============================================================================
  // Context Management
  // ============================================================================

  /**
   * Build a new CheckContext for the current operation.
   *
   * <p>Callers can use this to create a shared context across multiple batches.
   *
   * @param opContext operation context
   * @return new check context
   */
  public CheckContext buildContext(@Nonnull OperationContext opContext) {
    return CheckContext.builder()
        .operationContext(opContext)
        .entityService(entityService)
        .graphClient(graphClient)
        .checkConfigs(new HashMap<>(checkConfigs))
        .build();
  }

  // ============================================================================
  // Private Helpers
  // ============================================================================

  /**
   * Apply a fix for an issue using the appropriate fix implementation.
   *
   * @param opContext operation context
   * @param issue the issue to fix
   * @param dryRun if true, don't make actual changes
   * @return fix detail
   */
  private ConsistencyFixDetail applyFix(
      OperationContext opContext, ConsistencyIssue issue, boolean dryRun) {
    ConsistencyFix fix = fixRegistry.getFix(issue.getFixType()).orElse(null);
    if (fix == null) {
      log.error("No fix implementation for type {}", issue.getFixType());
      return ConsistencyFixDetail.builder()
          .urn(issue.getEntityUrn())
          .action(issue.getFixType())
          .success(false)
          .errorMessage("No fix implementation for type: " + issue.getFixType())
          .build();
    }
    return fix.apply(opContext, issue, dryRun);
  }
}
