package com.linkedin.metadata.search;

import static com.datahub.authorization.AuthUtil.canViewEntity;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;
import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.elastic.LineageBenchLog;
import com.linkedin.metadata.graph.inmemory.ImpactLineageSearchPushdown;
import com.linkedin.metadata.query.FreshnessStats;
import com.linkedin.metadata.query.GroupingCriterion;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SchemaFieldValidationMode;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.cache.CachedEntityLineageResult;
import com.linkedin.metadata.search.utils.FilterUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metadata.utils.metrics.CascadeOperationContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.cache.Cache;

@RequiredArgsConstructor
@Slf4j
public class LineageSearchService {

  private static final SearchFlags DEFAULT_SERVICE_SEARCH_FLAGS =
      new SearchFlags()
          .setFulltext(false)
          .setMaxAggValues(20)
          .setSkipCache(false)
          .setSkipAggregates(false)
          .setSkipHighlighting(true)
          .setIncludeRestricted(false)
          .setGroupingSpec(
              new GroupingSpec()
                  .setGroupingCriteria(
                      new GroupingCriterionArray(
                          new GroupingCriterion() // Convert schema fields to datasets by default to
                              // maintain backwards compatibility
                              .setBaseEntityType(SCHEMA_FIELD_ENTITY_NAME)
                              .setGroupingEntityType(DATASET_ENTITY_NAME))));

  private final SearchService _searchService;
  private final GraphService _graphService;
  @Nullable private final Cache cache;
  private final boolean cacheEnabled;
  private final DataHubAppConfiguration appConfig;
  private final ExecutorService cacheRefillExecutor = Executors.newFixedThreadPool(1);

  @Setter @Nullable private MetricUtils metricUtils;

  private static final String DEGREE_FILTER = "degree";

  // Tags on datahub.lineage.duration; values are bounded enums to keep series count low
  private static final String TAG_KIND = "kind";
  private static final String TAG_SEARCH_PATH = "search_path";
  private static final String TAG_CACHE = "cache";
  private static final String KIND_IMPACT = "impact";
  private static final String KIND_VISUALIZATION = "visualization";
  private static final String SEARCH_PATH_LIGHTNING = "lightning";
  private static final String SEARCH_PATH_TORTOISE = "tortoise";
  // The request failed before a path was chosen
  private static final String SEARCH_PATH_UNKNOWN = "unknown";
  private static final String CACHE_HIT = "hit";
  private static final String CACHE_MISS = "miss";
  // Cache disabled, or the caller set skipCache
  private static final String CACHE_SKIP = "skip";
  // A rejected request is an outcome on datahub.lineage.outcomes, not an error
  private static final String OUTCOME_INVALID_REQUEST = "invalid_request";
  // error_type on datahub.lineage.errors for anything else that threw
  private static final String ERROR_FAILURE = "failure";
  // Stages timed on the lineage log line only
  private static final String PHASE_WALK = "walk";
  private static final String PHASE_FILTER = "filter";
  private static final String PHASE_SEARCH = "search";

  /** Filter fields the graph-only path can answer from a urn, without the entity index. */
  private static final Set<String> LIGHTNING_FILTER_FIELDS = LightningUrnFields.FILTER_FIELDS;

  private static final AggregationMetadata DEGREE_FILTER_GROUP =
      new AggregationMetadata()
          .setName(DEGREE_FILTER)
          .setDisplayName("Degree of Dependencies")
          .setAggregations(new LongMap())
          .setFilterValues(
              new FilterValueArray(
                  ImmutableList.of(
                      new FilterValue().setValue("1").setFacetCount(0),
                      new FilterValue().setValue("2").setFacetCount(0),
                      new FilterValue().setValue("3+").setFacetCount(0))));

  private static final int MAX_TERMS = 50000;

  /**
   * Gets a list of documents that match given search request that is related to the input entity
   *
   * @param sourceUrn Urn of the source entity
   * @param direction Direction of the relationship
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param maxHops the maximum number of hops away to search for. If null, defaults to the
   *     configured per-mode maximum (impact.maxHops for impact analysis; lineageMaxHops for
   *     visualization), and any larger value is clamped to it
   * @param inputFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a {@link LineageSearchResult} that contains a list of matched documents and related
   *     search result metadata
   */
  @Nonnull
  @WithSpan
  public LineageSearchResult searchAcrossLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter inputFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {

    final long benchStart = System.nanoTime();
    LineageBenchLog.clearLastWalk();
    // Not try-with-resources: the failure must be recorded before close() emits the timer and log
    final CascadeOperationContext cascade =
        beginLineageOperation(opContext, "searchAcrossLineage", sourceUrn, SEARCH_PATH_UNKNOWN);
    try {
      final String finalInput = input == null || input.isEmpty() ? "*" : input;

      final OperationContext finalOpContext =
          opContext
              .withSearchFlags(
                  flags -> applyDefaultSearchFlags(flags, finalInput, DEFAULT_SERVICE_SEARCH_FLAGS))
              .withLineageFlags(lineageFlags -> lineageFlags);

      log.debug(
          "Cache enabled {}, Input :{}:",
          enableCache(finalOpContext.getSearchContext().getSearchFlags()),
          finalInput);
      maxHops = applyMaxHopsLimit(opContext.getSearchContext().getLineageFlags(), maxHops);

      // Cache multihop result for faster performance
      final EntityLineageResultCacheKey cacheKey =
          new EntityLineageResultCacheKey(
              finalOpContext.getSearchContextId(),
              sourceUrn,
              direction,
              maxHops,
              opContext.getSearchContext().getLineageFlags() != null
                  ? opContext.getSearchContext().getLineageFlags().getEntitiesExploredPerHopLimit()
                  : null);
      CachedEntityLineageResult cachedLineageResult = null;

      if (enableCache(finalOpContext.getSearchContext().getSearchFlags())) {
        try {
          cachedLineageResult = cache.get(cacheKey, CachedEntityLineageResult.class);
        } catch (Exception e) {
          log.warn("Failed to load cacheKey {}", cacheKey, e);
        }
        cascade.tag(TAG_CACHE, cachedLineageResult == null ? CACHE_MISS : CACHE_HIT);
      }

      EntityLineageResult lineageResult;
      FreshnessStats freshnessStats = new FreshnessStats().setCached(Boolean.FALSE);
      long benchWalkNanos = 0L;
      if (cachedLineageResult == null) {
        final long benchWalkStart = System.nanoTime();
        // A walk this request will not cache can be searched next to the graph instead, which
        // returns only the page; the full walk comes back only when the lightning path won't apply.
        ImpactLineageSearchPushdown.Outcome pushed =
            enableCache(finalOpContext.getSearchContext().getSearchFlags())
                ? null
                : pushDownSearch(
                    opContext,
                    finalOpContext,
                    sourceUrn,
                    direction,
                    maxHops,
                    entities,
                    finalInput,
                    inputFilters,
                    sortCriteria,
                    from,
                    size);
        if (pushed instanceof ImpactLineageSearchPushdown.Lightning lightning) {
          final long benchResultStart = System.nanoTime();
          cascade.recordEntitiesProcessed(lightning.prefiltered());
          LineageSearchResult lineageSearchResult = lightningSearchResult(lightning, from, size);
          LineageBenchLog.search(
              LineageBenchLog.lastWalkId(),
              benchResultStart - benchWalkStart,
              0L,
              System.nanoTime() - benchResultStart,
              "lightning",
              lightning.prefiltered(),
              System.nanoTime() - benchStart);
          return lineageSearchResult;
        }
        lineageResult =
            pushed instanceof ImpactLineageSearchPushdown.Full full
                ? full.result()
                : getLineageResult(opContext, sourceUrn, direction, maxHops);
        benchWalkNanos = System.nanoTime() - benchWalkStart;
        cascade.recordPhase(PHASE_WALK, benchWalkNanos);

        if (enableCache(finalOpContext.getSearchContext().getSearchFlags())) {
          try {
            cache.put(
                cacheKey, new CachedEntityLineageResult(lineageResult, System.currentTimeMillis()));
          } catch (Exception e) {
            log.warn("Failed to add cacheKey {}", cacheKey, e);
          }
        }
      } else {
        lineageResult = cachedLineageResult.getEntityLineageResult();
        freshnessStats.setCached(Boolean.TRUE);
        LongMap systemFreshness = new LongMap();
        systemFreshness.put("LineageGraphCache", cachedLineageResult.getTimestamp());
        freshnessStats.setSystemFreshness(systemFreshness);
        // set up cache refill if needed
        if (System.currentTimeMillis() - cachedLineageResult.getTimestamp()
            > appConfig.getCache().getSearch().getLineage().getTTLMillis()) {
          log.info("Cached lineage entry for: {} is older than one day. Will refill.", sourceUrn);
          Integer finalMaxHops = maxHops;
          this.cacheRefillExecutor.submit(
              () -> {
                log.debug("Cache refill started.");
                CachedEntityLineageResult reFetchLineageResult =
                    cache.get(cacheKey, CachedEntityLineageResult.class);
                if (reFetchLineageResult == null
                    || System.currentTimeMillis() - reFetchLineageResult.getTimestamp()
                        > appConfig.getCache().getSearch().getLineage().getTTLMillis()) {
                  // we have to refetch
                  EntityLineageResult result =
                      getLineageResult(opContext, sourceUrn, direction, finalMaxHops);
                  if (enableCache(finalOpContext.getSearchContext().getSearchFlags())) {
                    cache.put(cacheKey, result);
                  }
                  log.debug("Refilled Cached lineage entry for: {}.", sourceUrn);
                } else {
                  log.debug(
                      "Cache refill not needed. {}",
                      System.currentTimeMillis() - reFetchLineageResult.getTimestamp());
                }
              });
        }
      }

      long filterStart = System.nanoTime();
      final long benchFilterStart = filterStart;
      if (SearchUtils.convertSchemaFieldToDataset(
          finalOpContext.getSearchContext().getSearchFlags())) {
        // set schemaField relationship entity to be its reference urn
        LineageRelationshipArray updatedRelationships =
            convertSchemaFieldRelationships(lineageResult);
        lineageResult.setRelationships(updatedRelationships);
      }

      // Filter hopped result based on the set of entities to return and inputFilters before sending
      // to search
      List<LineageRelationship> lineageRelationships =
          filterRelationships(lineageResult, new HashSet<>(entities), inputFilters);
      log.debug("Lineage relationships found: {}", lineageRelationships);
      cascade.recordPhase(PHASE_FILTER, System.nanoTime() - filterStart);
      cascade.recordEntitiesProcessed(lineageRelationships.size());
      final long benchResultStart = System.nanoTime();
      final long benchFilterNanos = benchResultStart - benchFilterStart;

      Filter reducedFilters =
          SearchUtils.removeCriteria(
              inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));

      boolean forceLightningMode =
          Optional.ofNullable(finalOpContext.getSearchContext().getLineageFlags())
              .map(LineageFlags::isForceLightningMode)
              .orElse(false);

      if (canDoLightning(
          lineageRelationships, finalInput, reducedFilters, sortCriteria, forceLightningMode)) {
        cascade.tag(TAG_SEARCH_PATH, SEARCH_PATH_LIGHTNING);
        long searchStart = System.nanoTime();
        // use lightning approach to return lineage search results
        List<LineageRelationship> countable =
            dropSchemaFieldsMissingFromParent(finalOpContext, lineageRelationships);
        LineageSearchResult lineageSearchResult =
            getLightningSearchResult(
                countable, reducedFilters, from, size, new HashSet<>(entities));
        cascade.recordPhase(PHASE_SEARCH, System.nanoTime() - searchStart);
        if (!lineageSearchResult.getEntities().isEmpty()) {
          log.debug(
              "Lightning Lineage entity result: {}", lineageSearchResult.getEntities().get(0));
        }
        lineageSearchResult.setLineageSearchPath(LineageSearchPath.LIGHTNING);
        if (lineageResult.hasPartial()) {
          lineageSearchResult.setIsPartial(lineageResult.isPartial());
        }
        LineageBenchLog.search(
            LineageBenchLog.lastWalkId(),
            benchWalkNanos,
            benchFilterNanos,
            System.nanoTime() - benchResultStart,
            "lightning",
            lineageRelationships.size(),
            System.nanoTime() - benchStart);
        return lineageSearchResult;
      } else if (forceLightningMode) {
        // Falling through would answer from the entity index, which has nothing to return for an
        // entity that does not exist -- so the caller would silently get a short result rather than
        // what it asked for
        throw new IllegalArgumentException(
            unservableLightningMessage(finalInput, sortCriteria, reducedFilters));
      } else {
        cascade.tag(TAG_SEARCH_PATH, SEARCH_PATH_TORTOISE);
        long searchStart = System.nanoTime();
        LineageSearchResult lineageSearchResult =
            getSearchResultInBatches(
                finalOpContext,
                lineageRelationships,
                finalInput,
                reducedFilters,
                sortCriteria,
                from,
                size);
        cascade.recordPhase(PHASE_SEARCH, System.nanoTime() - searchStart);
        if (!lineageSearchResult.getEntities().isEmpty()) {
          log.debug(
              "Lineage entity results number -> {}; first -> {}",
              lineageSearchResult.getNumEntities(),
              lineageSearchResult.getEntities().get(0));
        }
        lineageSearchResult.setLineageSearchPath(LineageSearchPath.TORTOISE);
        if (lineageResult.hasPartial()) {
          lineageSearchResult.setIsPartial(lineageResult.isPartial());
        }
        LineageBenchLog.search(
            LineageBenchLog.lastWalkId(),
            benchWalkNanos,
            benchFilterNanos,
            System.nanoTime() - benchResultStart,
            "tortoise",
            lineageRelationships.size(),
            System.nanoTime() - benchStart);
        return lineageSearchResult;
      }
    } catch (RuntimeException e) {
      recordFailure(cascade, e);
      throw e;
    } finally {
      cascade.close();
    }
  }

  @VisibleForTesting
  boolean canDoLightning(
      List<LineageRelationship> lineageRelationships,
      String input,
      Filter inputFilters,
      List<SortCriterion> sortCriteria,
      boolean forceLightningMode) {
    boolean simpleFilters = hasOnlyLightningFilters(inputFilters);
    boolean worthwhile =
        forceLightningMode
            || lineageRelationships.size()
                > appConfig.getCache().getSearch().getLineage().getLightningThreshold();
    return worthwhile
        && input.equals("*")
        && simpleFilters
        && CollectionUtils.isEmpty(sortCriteria);
  }

  private static boolean hasOnlyLightningFilters(@Nullable Filter inputFilters) {
    return inputFilters == null
        || inputFilters.getOr() == null
        || inputFilters.getOr().stream()
            .allMatch(
                criterion ->
                    criterion.getAnd().stream()
                        .allMatch(
                            criterion1 -> LIGHTNING_FILTER_FIELDS.contains(criterion1.getField())));
  }

  /**
   * Hands the lightning search to a graph service that can run it next to the graph, when this
   * request is one the lightning path could serve. Returns null to walk and search here instead:
   * when the graph service cannot, the request might not take the lightning path, or anything about
   * it would fail here (so that it fails exactly as it would have).
   */
  @Nullable
  private ImpactLineageSearchPushdown.Outcome pushDownSearch(
      @Nonnull OperationContext opContext,
      @Nonnull OperationContext finalOpContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      int maxHops,
      @Nonnull List<String> entities,
      @Nonnull String finalInput,
      @Nullable Filter inputFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    if (!(_graphService instanceof ImpactLineageSearchPushdown pushdown)
        || isLineageVisualization(opContext.getSearchContext().getLineageFlags())
        || !"*".equals(finalInput)
        || !CollectionUtils.isEmpty(sortCriteria)) {
      return null;
    }
    final ImpactLineageSearchPushdown.Spec spec;
    try {
      LineageFlags lineageFlags = finalOpContext.getSearchContext().getLineageFlags();
      // Schema field validation reads the primary store, which the graph server cannot
      if (!SchemaFieldValidationMode.NONE.equals(schemaFieldValidationMode(lineageFlags))) {
        return null;
      }
      Filter reducedFilters =
          SearchUtils.removeCriteria(
              inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));
      if (!hasOnlyLightningFilters(reducedFilters)) {
        return null;
      }
      List<String> degreeValues = degreeFilterValues(inputFilters);
      if (!degreeValues.isEmpty()) {
        convertFilterToPredicate(degreeValues); // rejects values it does not know
      }
      spec =
          new ImpactLineageSearchPushdown.Spec(
              new HashSet<>(entities),
              degreeValues,
              reducedFilters,
              SearchUtils.convertSchemaFieldToDataset(
                  finalOpContext.getSearchContext().getSearchFlags()),
              Optional.ofNullable(lineageFlags)
                  .map(LineageFlags::isForceLightningMode)
                  .orElse(false),
              appConfig.getCache().getSearch().getLineage().getLightningThreshold(),
              from,
              ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), size));
    } catch (RuntimeException e) {
      return null;
    }
    return pushdown
        .searchImpactLineage(
            opContext,
            sourceUrn,
            LineageGraphFilters.forEntityType(
                opContext.getLineageRegistry(), sourceUrn.getEntityType(), direction),
            maxHops,
            spec)
        .orElse(null);
  }

  /**
   * The lightning result for a search the graph service ran, as getLightningSearchResult builds it.
   */
  private LineageSearchResult lightningSearchResult(
      ImpactLineageSearchPushdown.Lightning lightning, int from, @Nullable Integer size) {
    LineageSearchEntityArray lineageSearchEntityArray =
        new LineageSearchEntityArray(lightning.rows().size());
    for (ImpactLineageSearchPushdown.Row row : lightning.rows()) {
      lineageSearchEntityArray.add(
          new LineageSearchEntity()
              .setEntity(row.entity())
              .setDegree(row.degree())
              .setPaths(row.paths()));
    }
    LineageSearchResult finalResult =
        new LineageSearchResult().setMetadata(new SearchResultMetadata());
    finalResult.setEntities(lineageSearchEntityArray);
    finalResult
        .getMetadata()
        .setAggregations(
            lightningAggregations(
                tally(lightning.platforms()),
                tally(lightning.entityTypes()),
                tally(lightning.origins())));
    finalResult.setNumEntities(lightning.total());
    finalResult
        .setFrom(from)
        .setPageSize(ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), size));
    finalResult.setLineageSearchPath(LineageSearchPath.LIGHTNING);
    finalResult.setIsPartial(lightning.partial());
    return finalResult;
  }

  /**
   * Facet counts as the HashMap getLightningSearchResult tallies them into, whose iteration order
   * is the order the facet values are shown in. That order depends on the order keys were added and
   * on when the map grew, so the keys are added in the order they were first met, and the map is
   * touched once more if counting carried on after the last new key (which is when it may grow).
   */
  private static Map<String, Long> tally(ImpactLineageSearchPushdown.Facet facet) {
    Map<String, Long> counts = new HashMap<>();
    String last = null;
    for (Map.Entry<String, Long> e : facet.counts().entrySet()) {
      Long count = e.getValue();
      counts.compute(e.getKey(), (key, value) -> count);
      last = e.getKey();
    }
    if (facet.countedAfterLastNew() && last != null) {
      counts.compute(last, (key, value) -> value);
    }
    return counts;
  }

  /**
   * Drops schema fields the graph points at that their parent no longer declares, which the graph
   * keeps edges for long after a column is removed or its dataset deleted. Reads schemaMetadata for
   * the parents in one batch and keeps only fields it still lists, under any of the urn aliases a
   * field can be referred to by.
   *
   * <p>Relationships that are not schema fields are left alone: this says nothing about whether
   * they exist.
   */
  @VisibleForTesting
  List<LineageRelationship> dropSchemaFieldsMissingFromParent(
      @Nonnull OperationContext opContext, List<LineageRelationship> relationships) {
    final SchemaFieldValidationMode mode =
        schemaFieldValidationMode(opContext.getSearchContext().getLineageFlags());
    if (SchemaFieldValidationMode.NONE.equals(mode)) {
      return relationships;
    }

    final Set<Urn> schemaFields = new HashSet<>();
    final Set<Urn> parents = new HashSet<>();
    for (LineageRelationship relationship : relationships) {
      SchemaFieldUtils.parseSchemaFieldUrn(relationship.getEntity())
          .ifPresent(
              parsed -> {
                schemaFields.add(relationship.getEntity());
                parents.add(parsed.getFirst());
              });
    }

    if (parents.isEmpty()) {
      return relationships;
    }
    final int maxParentsToValidate =
        appConfig.getSearchService().getLineage().getMaxParentsToValidate();
    if (SchemaFieldValidationMode.AUTO.equals(mode) && parents.size() > maxParentsToValidate) {
      log.info(
          "Skipping schema field validation for {} parents, above the limit of {}. Request ALWAYS to"
              + " validate regardless.",
          parents.size(),
          maxParentsToValidate);
      return relationships;
    }

    final Map<Urn, Map<String, Aspect>> aspects =
        opContext
            .getRetrieverContext()
            .getAspectRetriever()
            .getLatestAspectObjects(opContext, parents, Set.of(SCHEMA_METADATA_ASPECT_NAME));

    final Set<Urn> declared = new HashSet<>();
    for (Urn parent : parents) {
      final Aspect aspect =
          Optional.ofNullable(aspects.get(parent))
              .map(a -> a.get(SCHEMA_METADATA_ASPECT_NAME))
              .orElse(null);
      if (aspect == null) {
        // No schema to check against, so nothing under this parent can be confirmed to exist
        continue;
      }
      final SchemaMetadata schemaMetadata =
          RecordUtils.toRecordTemplate(SchemaMetadata.class, aspect.data());
      for (SchemaField field : schemaMetadata.getFields()) {
        declared.addAll(SchemaFieldUtils.getSchemaFieldAliases(parent, schemaMetadata, field));
        declared.add(SchemaFieldUtils.generateSchemaFieldUrn(parent, field));
      }
    }

    // Preserves the incoming order, which the caller pages over
    return relationships.stream()
        .filter(
            relationship ->
                !schemaFields.contains(relationship.getEntity())
                    || declared.contains(relationship.getEntity()))
        .collect(Collectors.toList());
  }

  /**
   * How much to spend checking that the schema fields counted off the graph still exist. Defaults
   * to NONE so that callers which never asked for it are unaffected, including those that reach the
   * graph-only path merely by exceeding its result-size threshold.
   */
  @VisibleForTesting
  static SchemaFieldValidationMode schemaFieldValidationMode(@Nullable LineageFlags lineageFlags) {
    return Optional.ofNullable(lineageFlags)
        .map(LineageFlags::getValidateSchemaFields)
        .filter(mode -> !SchemaFieldValidationMode.$UNKNOWN.equals(mode))
        .orElse(SchemaFieldValidationMode.NONE);
  }

  private static void rejectUnsupportedScrollFlags(@Nullable LineageFlags lineageFlags) {
    if (lineageFlags == null) {
      return;
    }
    if (Boolean.TRUE.equals(lineageFlags.isForceLightningMode())) {
      throw new IllegalArgumentException(
          "forceLightningMode is not supported by scrollAcrossLineage: "
              + "lightning mode is only a feature of searchAcrossLineage.");
    }
    final SchemaFieldValidationMode mode = schemaFieldValidationMode(lineageFlags);
    if (!SchemaFieldValidationMode.NONE.equals(mode)) {
      throw new IllegalArgumentException(
          String.format(
              "validateSchemaFields=%s is not supported by scrollAcrossLineage. "
                  + "Use searchAcrossLineage instead.",
              mode));
    }
  }

  private static String unservableLightningMessage(
      String input, @Nullable List<SortCriterion> sortCriteria, @Nullable Filter filters) {
    return String.format(
        "forceLightningMode reads results off the lineage graph, which cannot serve this query. It "
            + "needs a '*' query, no sort criteria, and filters only on %s, but got query '%s', %d "
            + "sort criteria, and filters on %s.",
        LIGHTNING_FILTER_FIELDS,
        input,
        sortCriteria == null ? 0 : sortCriteria.size(),
        filterFields(filters));
  }

  /** The distinct fields the filters constrain, sorted so the message reads the same every time. */
  private static Set<String> filterFields(@Nullable Filter filters) {
    if (filters == null || filters.getOr() == null) {
      return Set.of();
    }
    return filters.getOr().stream()
        .map(ConjunctiveCriterion::getAnd)
        .flatMap(CriterionArray::stream)
        .map(Criterion::getField)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /** Whether a urn passes the lightning filters; see {@link LightningUrnFields#passes}. */
  @VisibleForTesting
  static boolean passesLightningCriteria(
      Urn urn, @Nullable String platform, @Nullable String environment, @Nullable Filter filters) {
    return LightningUrnFields.passes(
        LightningUrnFields.parent(urn), platform, environment, filters);
  }

  @VisibleForTesting
  LineageSearchResult getLightningSearchResult(
      List<LineageRelationship> lineageRelationships,
      Filter inputFilters,
      int from,
      @Nullable Integer size,
      Set<String> entityNames) {
    size = ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), size);

    // Construct result objects
    LineageSearchResult finalResult =
        new LineageSearchResult().setMetadata(new SearchResultMetadata());
    LineageSearchEntityArray lineageSearchEntityArray = new LineageSearchEntityArray();
    Map<String, Long> entityTypeAggregations = new HashMap<>();
    Map<String, Long> platformTypeAggregations = new HashMap<>();
    Map<String, Long> environmentAggregations = new HashMap<>();

    // Aggregations supported by this model
    // entity type
    // platform
    // environment
    int start = 0;
    int numElements = 0;
    for (LineageRelationship relnship : lineageRelationships) {
      Urn entityUrn = relnship.getEntity();
      String entityType = entityUrn.getEntityType();

      String platform = getPlatform(entityType, entityUrn);
      String environment = getEnvironment(entityType, entityUrn);

      boolean isNotFiltered =
          (entityNames.isEmpty() || entityNames.contains(entityType))
              && passesLightningCriteria(entityUrn, platform, environment, inputFilters);

      if (isNotFiltered) {
        start++;
        if ((start > from) && (numElements < size)) {
          lineageSearchEntityArray.add(
              new LineageSearchEntity()
                  .setEntity(entityUrn)
                  .setDegree(relnship.getDegree())
                  .setPaths(relnship.getPaths()));
          numElements++;
        }

        // entityType
        entityTypeAggregations.compute(entityType, (key, value) -> value == null ? 1L : ++value);

        // platform
        if (platform != null) {
          platformTypeAggregations.compute(platform, (key, value) -> value == null ? 1L : ++value);
        }

        // environment
        if (environment != null) {
          environmentAggregations.compute(
              environment, (key, value) -> value == null ? 1L : ++value);
        }
      }
    }

    finalResult.setEntities(lineageSearchEntityArray);
    finalResult
        .getMetadata()
        .setAggregations(
            lightningAggregations(
                platformTypeAggregations, entityTypeAggregations, environmentAggregations));
    finalResult.setNumEntities(start);
    return finalResult.setFrom(from).setPageSize(size);
  }

  /**
   * The lightning facets, in the order the UI shows them. Each map's iteration order decides the
   * order of its values, so callers fill them in the order the matching urns are met.
   */
  private AggregationMetadataArray lightningAggregations(
      Map<String, Long> platformTypeAggregations,
      Map<String, Long> entityTypeAggregations,
      Map<String, Long> environmentAggregations) {
    AggregationMetadata entityTypeAgg = constructAggMetadata("Type", "entity");
    AggregationMetadata platformTypeAgg = constructAggMetadata("Platform", "platform");
    AggregationMetadata environmentAgg = constructAggMetadata("Environment", "origin");
    AggregationMetadataArray aggregationMetadataArray = new AggregationMetadataArray();

    aggregationMetadataArray.add(DEGREE_FILTER_GROUP);
    if (platformTypeAggregations.keySet().size() > 0) {
      for (Map.Entry<String, Long> platformCount : platformTypeAggregations.entrySet()) {
        try {
          platformTypeAgg
              .getFilterValues()
              .add(
                  new FilterValue()
                      .setValue(platformCount.getKey())
                      .setFacetCount(platformCount.getValue())
                      .setEntity(Urn.createFromString(platformCount.getKey())));
          platformTypeAgg.getAggregations().put(platformCount.getKey(), platformCount.getValue());
        } catch (URISyntaxException e) {
          log.warn("Unexpected exception: {}", e.getMessage());
        }
      }
      aggregationMetadataArray.add(platformTypeAgg);
    }
    if (entityTypeAggregations.keySet().size() > 0) {
      for (Map.Entry<String, Long> entityCount : entityTypeAggregations.entrySet()) {
        entityTypeAgg
            .getFilterValues()
            .add(
                new FilterValue()
                    .setValue(entityCount.getKey())
                    .setFacetCount(entityCount.getValue()));
        entityTypeAgg.getAggregations().put(entityCount.getKey(), entityCount.getValue());
      }
      aggregationMetadataArray.add(entityTypeAgg);
    }
    if (environmentAggregations.keySet().size() > 0) {
      for (Map.Entry<String, Long> entityCount : environmentAggregations.entrySet()) {
        environmentAgg
            .getFilterValues()
            .add(
                new FilterValue()
                    .setValue(entityCount.getKey())
                    .setFacetCount(entityCount.getValue()));
        environmentAgg.getAggregations().put(entityCount.getKey(), entityCount.getValue());
      }
      aggregationMetadataArray.add(environmentAgg);
    }
    return aggregationMetadataArray;
  }

  private AggregationMetadata constructAggMetadata(String displayName, String name) {
    return new AggregationMetadata()
        .setDisplayName(displayName)
        .setName(name)
        .setAggregations(new LongMap())
        .setFilterValues(new FilterValueArray());
  }

  @VisibleForTesting
  String getPlatform(String entityType, Urn entityUrn) {
    return LightningUrnFields.platform(entityType, entityUrn);
  }

  @VisibleForTesting
  @Nullable
  String getEnvironment(String entityType, Urn entityUrn) {
    return LightningUrnFields.environment(entityType, entityUrn);
  }

  // Necessary so we don't filter out schemaField entities and so that we search to get the parent
  // reference entity
  private LineageRelationshipArray convertSchemaFieldRelationships(
      EntityLineageResult lineageResult) {
    return lineageResult.getRelationships().stream()
        .map(
            relationship -> {
              if (relationship.getEntity().getEntityType().equals("schemaField")) {
                Urn entity = getSchemaFieldReferenceUrn(relationship.getEntity());
                relationship.setEntity(entity);
              }
              return relationship;
            })
        .collect(Collectors.toCollection(LineageRelationshipArray::new));
  }

  private Map<Urn, LineageRelationship> generateUrnToRelationshipMap(
      List<LineageRelationship> lineageRelationships) {
    Map<Urn, LineageRelationship> urnToRelationship = new HashMap<>();
    for (LineageRelationship relationship : lineageRelationships) {
      LineageRelationship existingRelationship = urnToRelationship.get(relationship.getEntity());
      if (existingRelationship == null) {
        urnToRelationship.put(relationship.getEntity(), relationship);
      } else {
        UrnArrayArray newPaths =
            new UrnArrayArray(
                existingRelationship.getPaths().size() + relationship.getPaths().size());
        log.debug(
            "Found {} paths for {}, will add to existing paths: {}",
            relationship.getPaths().size(),
            relationship.getEntity(),
            existingRelationship.getPaths().size());
        newPaths.addAll(existingRelationship.getPaths());
        newPaths.addAll(relationship.getPaths());
        existingRelationship.setPaths(newPaths);
      }
    }
    return urnToRelationship;
  }

  // Search service can only take up to 50K term filter, so query search service in batches
  private LineageSearchResult getSearchResultInBatches(
      @Nonnull OperationContext opContext,
      List<LineageRelationship> lineageRelationships,
      @Nonnull String input,
      @Nullable Filter inputFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    size = ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), size);
    LineageSearchResult finalResult =
        new LineageSearchResult()
            .setEntities(new LineageSearchEntityArray(Collections.emptyList()))
            .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()))
            .setFrom(from)
            .setPageSize(size)
            .setNumEntities(0);
    List<List<LineageRelationship>> batchedRelationships =
        Lists.partition(lineageRelationships, MAX_TERMS);
    int queryFrom = from;
    int querySize = size;
    for (List<LineageRelationship> batch : batchedRelationships) {
      List<String> entitiesToQuery =
          batch.stream()
              .map(relationship -> relationship.getEntity().getEntityType())
              .distinct()
              .collect(Collectors.toList());
      Map<Urn, LineageRelationship> urnToRelationship = generateUrnToRelationshipMap(batch);
      Filter finalFilter = buildFilterWithUrns(appConfig, urnToRelationship.keySet(), inputFilters);

      LineageSearchResult resultForBatch =
          buildLineageSearchResult(
              opContext,
              _searchService.searchAcrossEntities(
                  opContext.withSearchFlags(
                      flags -> applyDefaultSearchFlags(flags, input, DEFAULT_SERVICE_SEARCH_FLAGS)),
                  entitiesToQuery,
                  input,
                  finalFilter,
                  sortCriteria,
                  queryFrom,
                  querySize),
              urnToRelationship);
      queryFrom = Math.max(0, from - resultForBatch.getNumEntities());
      querySize = Math.max(0, size - resultForBatch.getEntities().size());
      finalResult = merge(finalResult, resultForBatch);

      if (querySize == 0) {
        break;
      }
    }

    finalResult.getMetadata().getAggregations().add(0, DEGREE_FILTER_GROUP);
    return finalResult.setFrom(from).setPageSize(size);
  }

  @SneakyThrows
  public static LineageSearchResult merge(LineageSearchResult one, LineageSearchResult two) {
    LineageSearchResult finalResult = one.clone();
    finalResult.getEntities().addAll(two.getEntities());
    finalResult.setNumEntities(one.getNumEntities() + two.getNumEntities());

    Map<String, AggregationMetadata> aggregations =
        one.getMetadata().getAggregations().stream()
            .collect(Collectors.toMap(AggregationMetadata::getName, Function.identity()));
    two.getMetadata()
        .getAggregations()
        .forEach(
            metadata -> {
              if (aggregations.containsKey(metadata.getName())) {
                aggregations.put(
                    metadata.getName(),
                    SearchUtils.merge(aggregations.get(metadata.getName()), metadata));
              } else {
                aggregations.put(metadata.getName(), metadata);
              }
            });
    finalResult
        .getMetadata()
        .setAggregations(new AggregationMetadataArray(FilterUtils.rankFilterGroups(aggregations)));
    return finalResult;
  }

  private Predicate<Integer> convertFilterToPredicate(List<String> degreeFilterValues) {
    return degreeFilterValues.stream()
        .map(
            value -> {
              switch (value) {
                case "1":
                  return (Predicate<Integer>) (Integer numHops) -> (numHops == 1);
                case "2":
                  return (Predicate<Integer>) (Integer numHops) -> (numHops == 2);
                case "3+":
                  return (Predicate<Integer>) (Integer numHops) -> (numHops > 2);
                default:
                  throw new IllegalArgumentException(
                      String.format("%s is not a valid filter value for degree filters", value));
              }
            })
        .reduce(x -> false, Predicate::or);
  }

  private Urn getSchemaFieldReferenceUrn(Urn urn) {
    return LightningUrnFields.schemaFieldReferenceUrn(urn);
  }

  private List<LineageRelationship> filterRelationships(
      @Nonnull EntityLineageResult lineageResult,
      @Nonnull Set<String> entities,
      @Nullable Filter inputFilters) {
    Stream<LineageRelationship> relationshipsFilteredByEntities =
        lineageResult.getRelationships().stream();
    if (!entities.isEmpty()) {
      relationshipsFilteredByEntities =
          relationshipsFilteredByEntities.filter(
              relationship -> entities.contains(relationship.getEntity().getEntityType()));
    }
    List<String> degreeFilter = degreeFilterValues(inputFilters);
    if (!degreeFilter.isEmpty()) {
      Predicate<Integer> degreePredicate = convertFilterToPredicate(degreeFilter);
      return relationshipsFilteredByEntities
          .filter(relationship -> degreePredicate.test(relationship.getDegree()))
          .collect(Collectors.toList());
    }
    return relationshipsFilteredByEntities.collect(Collectors.toList());
  }

  /** The degree filter's values; only the first or-branch is read. */
  private static List<String> degreeFilterValues(@Nullable Filter inputFilters) {
    if (inputFilters != null && !CollectionUtils.isEmpty(inputFilters.getOr())) {
      ConjunctiveCriterion conjunctiveCriterion = inputFilters.getOr().get(0);
      if (conjunctiveCriterion.hasAnd()) {
        return conjunctiveCriterion.getAnd().stream()
            .filter(criterion -> criterion.getField().equals(DEGREE_FILTER))
            .flatMap(c -> c.getValues().stream())
            .collect(Collectors.toList());
      }
    }
    return List.of();
  }

  private LineageSearchResult buildLineageSearchResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResult searchResult,
      Map<Urn, LineageRelationship> urnToRelationship) {
    AggregationMetadataArray aggregations =
        new AggregationMetadataArray(searchResult.getMetadata().getAggregations());
    return new LineageSearchResult()
        .setEntities(
            new LineageSearchEntityArray(
                searchResult.getEntities().stream()
                    .map(
                        searchEntity ->
                            buildLineageSearchEntity(
                                opContext,
                                searchEntity,
                                urnToRelationship.get(searchEntity.getEntity())))
                    .collect(Collectors.toList())))
        .setMetadata(new SearchResultMetadata().setAggregations(aggregations))
        .setFrom(searchResult.getFrom())
        .setPageSize(searchResult.getPageSize())
        .setNumEntities(searchResult.getNumEntities());
  }

  private LineageSearchEntity buildLineageSearchEntity(
      @Nonnull OperationContext opContext,
      @Nonnull SearchEntity searchEntity,
      @Nullable LineageRelationship lineageRelationship) {
    LineageSearchEntity entity = new LineageSearchEntity(searchEntity.data());
    if (lineageRelationship != null) {
      entity.setPaths(
          lineageRelationship.getPaths().stream()
              .filter(
                  urnArray ->
                      urnArray.stream()
                          .allMatch(
                              urn -> {
                                if (opContext
                                    .getOperationContextConfig()
                                    .getViewAuthorizationConfiguration()
                                    .isEnabled()) {
                                  return canViewEntity(opContext, urn);
                                }
                                return true;
                              }))
              .collect(Collectors.toCollection(UrnArrayArray::new)));
      entity.setDegree(lineageRelationship.getDegree());
      if (lineageRelationship.hasDegrees()) {
        entity.setDegrees(lineageRelationship.getDegrees());
      }
      entity.setExplored(Boolean.TRUE.equals(lineageRelationship.isExplored()));
      entity.setTruncatedChildren(Boolean.TRUE.equals(lineageRelationship.isTruncatedChildren()));
      entity.setIgnoredAsHop(Boolean.TRUE.equals(lineageRelationship.isIgnoredAsHop()));
    }
    return entity;
  }

  /**
   * Gets a list of documents that match given search request that is related to the input entity
   *
   * @param sourceUrn Urn of the source entity
   * @param direction Direction of the relationship
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param maxHops the maximum number of hops away to search for. If null, defaults to the
   *     configured per-mode maximum (impact.maxHops for impact analysis; lineageMaxHops for
   *     visualization), and any larger value is clamped to it
   * @param inputFilters the request map with fields and values as filters to be applied to search
   *     hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll identifier to pass to search service
   * @param size the number of search hits to return
   * @return a {@link LineageSearchResult} that contains a list of matched documents and related
   *     search result metadata
   */
  @Nonnull
  @WithSpan
  public LineageScrollResult scrollAcrossLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter inputFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nonnull String keepAlive,
      @Nullable Integer size) {
    // Scroll always answers from the entity index
    final CascadeOperationContext cascade =
        beginLineageOperation(opContext, "scrollAcrossLineage", sourceUrn, SEARCH_PATH_TORTOISE);
    try {
      rejectUnsupportedScrollFlags(opContext.getSearchContext().getLineageFlags());

      // Clamp hops the same way searchAcrossLineage does (see applyMaxHopsLimit), before building
      // the cache key. The scroll path previously defaulted to 1000 without clamping, letting a
      // caller-supplied maxHops drive an unbounded deep traversal; clamping before the key also
      // keeps scroll and search cache entries aligned for the same logical request.
      maxHops = applyMaxHopsLimit(opContext.getSearchContext().getLineageFlags(), maxHops);

      // Cache multihop result for faster performance
      final EntityLineageResultCacheKey cacheKey =
          new EntityLineageResultCacheKey(
              opContext.getSearchContextId(),
              sourceUrn,
              direction,
              maxHops,
              opContext.getSearchContext().getLineageFlags() != null
                  ? opContext.getSearchContext().getLineageFlags().getEntitiesExploredPerHopLimit()
                  : null);
      CachedEntityLineageResult cachedLineageResult = null;
      if (enableCache(opContext.getSearchContext().getSearchFlags())) {
        cachedLineageResult = cache.get(cacheKey, CachedEntityLineageResult.class);
        cascade.tag(TAG_CACHE, cachedLineageResult == null ? CACHE_MISS : CACHE_HIT);
      }
      EntityLineageResult lineageResult;
      if (cachedLineageResult == null) {
        long walkStart = System.nanoTime();
        lineageResult = getLineageResult(opContext, sourceUrn, direction, maxHops);
        cascade.recordPhase(PHASE_WALK, System.nanoTime() - walkStart);
        if (enableCache(opContext.getSearchContext().getSearchFlags())) {
          cache.put(
              cacheKey, new CachedEntityLineageResult(lineageResult, System.currentTimeMillis()));
        }
      } else {
        lineageResult = cachedLineageResult.getEntityLineageResult();
        if (System.currentTimeMillis() - cachedLineageResult.getTimestamp()
            > appConfig.getCache().getSearch().getLineage().getTTLMillis()) {
          log.warn("Cached lineage entry for: {} is older than one day.", sourceUrn);
        }
      }

      long filterStart = System.nanoTime();
      // set schemaField relationship entity to be its reference urn
      LineageRelationshipArray updatedRelationships =
          convertSchemaFieldRelationships(lineageResult);
      lineageResult.setRelationships(updatedRelationships);

      // Filter hopped result based on the set of entities to return and inputFilters before sending
      // to search
      List<LineageRelationship> lineageRelationships =
          filterRelationships(lineageResult, new HashSet<>(entities), inputFilters);
      cascade.recordPhase(PHASE_FILTER, System.nanoTime() - filterStart);
      cascade.recordEntitiesProcessed(lineageRelationships.size());

      Filter reducedFilters =
          SearchUtils.removeCriteria(
              inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));
      long searchStart = System.nanoTime();
      LineageScrollResult scrollResult =
          getScrollResultInBatches(
              opContext,
              lineageRelationships,
              input != null ? input : "*",
              reducedFilters,
              sortCriteria,
              scrollId,
              keepAlive,
              ConfigUtils.applyLimit(appConfig.getGraphService(), size));
      cascade.recordPhase(PHASE_SEARCH, System.nanoTime() - searchStart);
      if (lineageResult.hasPartial()) {
        scrollResult.setIsPartial(lineageResult.isPartial());
      }
      return scrollResult;
    } catch (RuntimeException e) {
      recordFailure(cascade, e);
      throw e;
    } finally {
      cascade.close();
    }
  }

  // Search service can only take up to 50K term filter, so query search service in batches
  private LineageScrollResult getScrollResultInBatches(
      @Nonnull OperationContext opContext,
      List<LineageRelationship> lineageRelationships,
      @Nonnull String input,
      @Nullable Filter inputFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nonnull String keepAlive,
      @Nullable Integer size) {

    OperationContext finalOpContext =
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, input, DEFAULT_SERVICE_SEARCH_FLAGS));
    size = ConfigUtils.applyLimit(_graphService.getGraphServiceConfig(), size);

    LineageScrollResult finalResult =
        new LineageScrollResult()
            .setEntities(new LineageSearchEntityArray(Collections.emptyList()))
            .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()))
            .setPageSize(size)
            .setNumEntities(0);
    List<List<LineageRelationship>> batchedRelationships =
        Lists.partition(lineageRelationships, MAX_TERMS);
    int querySize = size;
    for (List<LineageRelationship> batch : batchedRelationships) {
      List<String> entitiesToQuery =
          batch.stream()
              .map(relationship -> relationship.getEntity().getEntityType())
              .distinct()
              .collect(Collectors.toList());
      Map<Urn, LineageRelationship> urnToRelationship = generateUrnToRelationshipMap(batch);
      Filter finalFilter = buildFilterWithUrns(appConfig, urnToRelationship.keySet(), inputFilters);

      LineageScrollResult resultForBatch =
          buildLineageScrollResult(
              opContext,
              _searchService.scrollAcrossEntities(
                  finalOpContext,
                  entitiesToQuery,
                  input,
                  finalFilter,
                  sortCriteria,
                  scrollId,
                  keepAlive,
                  querySize,
                  List.of()),
              urnToRelationship);
      querySize = Math.max(0, size - resultForBatch.getEntities().size());
      finalResult = mergeScrollResult(finalResult, resultForBatch);

      if (querySize == 0) {
        break;
      }
    }

    finalResult.getMetadata().getAggregations().add(0, DEGREE_FILTER_GROUP);
    return finalResult.setPageSize(size);
  }

  private LineageScrollResult buildLineageScrollResult(
      @Nonnull OperationContext opContext,
      @Nonnull ScrollResult scrollResult,
      Map<Urn, LineageRelationship> urnToRelationship) {
    AggregationMetadataArray aggregations =
        new AggregationMetadataArray(scrollResult.getMetadata().getAggregations());
    LineageScrollResult lineageScrollResult =
        new LineageScrollResult()
            .setEntities(
                new LineageSearchEntityArray(
                    scrollResult.getEntities().stream()
                        .map(
                            searchEntity ->
                                buildLineageSearchEntity(
                                    opContext,
                                    searchEntity,
                                    urnToRelationship.get(searchEntity.getEntity())))
                        .collect(Collectors.toList())))
            .setMetadata(new SearchResultMetadata().setAggregations(aggregations))
            .setPageSize(scrollResult.getPageSize())
            .setNumEntities(scrollResult.getNumEntities());

    if (scrollResult.getScrollId() != null) {
      lineageScrollResult.setScrollId(scrollResult.getScrollId());
    }
    return lineageScrollResult;
  }

  @SneakyThrows
  public static LineageScrollResult mergeScrollResult(
      LineageScrollResult one, LineageScrollResult two) {
    LineageScrollResult finalResult = one.clone();
    finalResult.getEntities().addAll(two.getEntities());
    finalResult.setNumEntities(one.getNumEntities() + two.getNumEntities());

    Map<String, AggregationMetadata> aggregations =
        one.getMetadata().getAggregations().stream()
            .collect(Collectors.toMap(AggregationMetadata::getName, Function.identity()));
    two.getMetadata()
        .getAggregations()
        .forEach(
            metadata -> {
              if (aggregations.containsKey(metadata.getName())) {
                aggregations.put(
                    metadata.getName(),
                    SearchUtils.merge(aggregations.get(metadata.getName()), metadata));
              } else {
                aggregations.put(metadata.getName(), metadata);
              }
            });
    finalResult
        .getMetadata()
        .setAggregations(new AggregationMetadataArray(FilterUtils.rankFilterGroups(aggregations)));
    if (two.getScrollId() != null) {
      finalResult.setScrollId(two.getScrollId());
    }
    return finalResult;
  }

  private int applyMaxHopsLimit(
      @Nullable LineageFlags lineageFlags, @Nullable Integer inputMaxHops) {
    // Determine if we're in UI mode or impact analysis mode
    // Get the appropriate limit based on the mode
    int configLimit =
        isLineageVisualization(lineageFlags)
            ? appConfig.getElasticSearch().getSearch().getGraph().getLineageMaxHops()
            : appConfig.getElasticSearch().getSearch().getGraph().getImpact().getMaxHops();

    // Apply the limit (either the config limit or the minimum of config and input)
    int result = (inputMaxHops == null) ? configLimit : Math.min(configLimit, inputMaxHops);

    // Log a warning if we had to reduce the requested hops
    if (inputMaxHops != null && result < inputMaxHops) {
      log.warn("Requested maxHops {} exceeded limit {}.", inputMaxHops, result);
    }

    return result;
  }

  /**
   * Starts the request-level lineage timer ({@code datahub.lineage.duration}) with every tag key
   * set, since Prometheus needs one tag-key set per meter name. Cache and search path start at
   * their fallback values and are refined once known.
   */
  private CascadeOperationContext beginLineageOperation(
      @Nonnull OperationContext opContext,
      @Nonnull String operationType,
      @Nonnull Urn sourceUrn,
      @Nonnull String searchPath) {
    return CascadeOperationContext.begin(
            metricUtils, operationType, sourceUrn, -1, "datahub.lineage")
        .tag(
            TAG_KIND,
            isLineageVisualization(opContext.getSearchContext().getLineageFlags())
                ? KIND_VISUALIZATION
                : KIND_IMPACT)
        .tag(TAG_SEARCH_PATH, searchPath)
        .tag(TAG_CACHE, CACHE_SKIP);
  }

  private static void recordFailure(CascadeOperationContext cascade, RuntimeException e) {
    // Unsupported flags, an unservable forced-lightning query or a bad degree filter: the caller's
    // mistake, not a backend failure
    if (e instanceof IllegalArgumentException) {
      cascade.recordOutcome(OUTCOME_INVALID_REQUEST);
    } else {
      cascade.recordError(ERROR_FAILURE);
    }
  }

  /** Returns true if the cache should be used or skipped when fetching search results */
  private boolean enableCache(@Nullable final SearchFlags searchFlags) {
    return cacheEnabled && (searchFlags == null || !searchFlags.isSkipCache());
  }

  private static boolean isLineageVisualization(@Nullable LineageFlags lineageFlags) {
    if (lineageFlags == null) {
      return false;
    }

    boolean hasEntitiesExploredLimit =
        lineageFlags.getEntitiesExploredPerHopLimit() != null
            && lineageFlags.getEntitiesExploredPerHopLimit() > 0;
    boolean hasIgnoreAsHops =
        lineageFlags.getIgnoreAsHops() != null && !lineageFlags.getIgnoreAsHops().isEmpty();

    return hasEntitiesExploredLimit || hasIgnoreAsHops;
  }

  private EntityLineageResult getLineageResult(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      int maxHops) {
    boolean isLineageVisualization =
        isLineageVisualization(opContext.getSearchContext().getLineageFlags());
    if (isLineageVisualization) {
      return _graphService.getLineage(
          opContext,
          sourceUrn,
          direction,
          0,
          _graphService.getGraphServiceConfig().getLimit().getResults().getApiDefault(),
          maxHops);
    } else {
      return _graphService.getImpactLineage(
          opContext,
          sourceUrn,
          LineageGraphFilters.forEntityType(
              opContext.getLineageRegistry(), sourceUrn.getEntityType(), direction),
          maxHops);
    }
  }
}
