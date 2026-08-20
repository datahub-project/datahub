package com.linkedin.metadata.search;

import static com.datahub.authorization.AuthUtil.canViewEntity;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;
import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.query.FreshnessStats;
import com.linkedin.metadata.query.GroupingCriterion;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.SchemaFieldValidationMode;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
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
import java.util.function.BiFunction;
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
  private static final String PARENT_FILTER = "parent";

  /** Filter fields the graph-only path can answer from a urn, without the entity index. */
  private static final Set<String> LIGHTNING_FILTER_FIELDS =
      Set.of("platform", "origin", PARENT_FILTER);

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

  private static final Set<String> PLATFORM_ENTITY_TYPES =
      ImmutableSet.of(
          DATASET_ENTITY_NAME,
          CHART_ENTITY_NAME,
          DASHBOARD_ENTITY_NAME,
          DATA_FLOW_ENTITY_NAME,
          DATA_JOB_ENTITY_NAME);

  /**
   * Gets a list of documents that match given search request that is related to the input entity
   *
   * @param sourceUrn Urn of the source entity
   * @param direction Direction of the relationship
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param maxHops the maximum number of hops away to search for. If null, defaults to 1000
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

    try (CascadeOperationContext cascade =
        CascadeOperationContext.begin(
            metricUtils, "searchAcrossLineage", sourceUrn, -1, "datahub.lineage")) {
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
      }

      EntityLineageResult lineageResult;
      FreshnessStats freshnessStats = new FreshnessStats().setCached(Boolean.FALSE);
      if (cachedLineageResult == null) {
        lineageResult = getLineageResult(opContext, sourceUrn, direction, maxHops);

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
      cascade.recordEntitiesProcessed(lineageRelationships.size());

      String codePath = null;
      Filter reducedFilters =
          SearchUtils.removeCriteria(
              inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));

      boolean forceLightningMode =
          Optional.ofNullable(finalOpContext.getSearchContext().getLineageFlags())
              .map(LineageFlags::isForceLightningMode)
              .orElse(false);

      if (canDoLightning(
          lineageRelationships, finalInput, reducedFilters, sortCriteria, forceLightningMode)) {
        codePath = "lightning";
        // use lightning approach to return lineage search results
        List<LineageRelationship> countable =
            dropSchemaFieldsMissingFromParent(finalOpContext, lineageRelationships);
        LineageSearchResult lineageSearchResult =
            getLightningSearchResult(
                countable, reducedFilters, from, size, new HashSet<>(entities));
        if (!lineageSearchResult.getEntities().isEmpty()) {
          log.debug(
              "Lightning Lineage entity result: {}",
              lineageSearchResult.getEntities().get(0).toString());
        }
        log.debug("Lineage search code path: {}", codePath);
        lineageSearchResult.setLineageSearchPath(LineageSearchPath.LIGHTNING);
        if (lineageResult.hasPartial()) {
          lineageSearchResult.setIsPartial(lineageResult.isPartial());
        }
        return lineageSearchResult;
      } else if (forceLightningMode) {
        // Falling through would answer from the entity index, which has nothing to return for an
        // entity that does not exist -- so the caller would silently get a short result rather than
        // what it asked for
        throw new IllegalArgumentException(
            unservableLightningMessage(finalInput, sortCriteria, reducedFilters));
      } else {
        codePath = "tortoise";
        LineageSearchResult lineageSearchResult =
            getSearchResultInBatches(
                finalOpContext,
                lineageRelationships,
                finalInput,
                reducedFilters,
                sortCriteria,
                from,
                size);
        if (!lineageSearchResult.getEntities().isEmpty()) {
          log.debug(
              "Lineage entity results number -> {}; first -> {}",
              lineageSearchResult.getNumEntities(),
              lineageSearchResult.getEntities().get(0).toString());
        }
        log.debug("Lineage search code path: {}", codePath);
        lineageSearchResult.setLineageSearchPath(LineageSearchPath.TORTOISE);
        if (lineageResult.hasPartial()) {
          lineageSearchResult.setIsPartial(lineageResult.isPartial());
        }
        return lineageSearchResult;
      }
    } // end try-with-resources CascadeOperationContext
  }

  @VisibleForTesting
  boolean canDoLightning(
      List<LineageRelationship> lineageRelationships,
      String input,
      Filter inputFilters,
      List<SortCriterion> sortCriteria,
      boolean forceLightningMode) {
    boolean simpleFilters =
        inputFilters == null
            || inputFilters.getOr() == null
            || inputFilters.getOr().stream()
                .allMatch(
                    criterion ->
                        criterion.getAnd().stream()
                            .allMatch(
                                criterion1 ->
                                    LIGHTNING_FILTER_FIELDS.contains(criterion1.getField())));
    boolean worthwhile =
        forceLightningMode
            || lineageRelationships.size()
                > appConfig.getCache().getSearch().getLineage().getLightningThreshold();
    return worthwhile
        && input.equals("*")
        && simpleFilters
        && CollectionUtils.isEmpty(sortCriteria);
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

  /**
   * Whether a urn passes the filters, which the graph-only path answers from the urn alone. A
   * Filter is a disjunction of conjunctions, so the urn passes if every criterion of any one
   * or-branch passes -- evaluating each branch separately rather than pooling their criteria, which
   * would reject urns that satisfy one branch but not another.
   *
   * <p>Negation is honored, so excluding the columns of a node the graph draws folded into another
   * is expressible.
   */
  @VisibleForTesting
  static boolean passesLightningCriteria(
      Urn urn, @Nullable String platform, @Nullable String environment, @Nullable Filter filters) {
    if (filters == null || CollectionUtils.isEmpty(filters.getOr())) {
      return true;
    }
    return filters.getOr().stream()
        .anyMatch(
            branch ->
                !branch.hasAnd()
                    || branch.getAnd().stream()
                        .allMatch(
                            criterion -> passesCriterion(urn, platform, environment, criterion)));
  }

  private static boolean passesCriterion(
      Urn urn, @Nullable String platform, @Nullable String environment, Criterion criterion) {
    if (CollectionUtils.isEmpty(criterion.getValues())) {
      return true;
    }
    // Fields outside these are rejected by canDoLightning, which never lets them reach here
    final String value;
    switch (criterion.getField()) {
      case "platform":
        value = platform;
        break;
      case "origin":
        value = environment;
        break;
      case PARENT_FILTER:
        value =
            SchemaFieldUtils.parseSchemaFieldUrn(urn)
                .map(parsed -> parsed.getFirst().toString())
                .orElse(null);
        break;
      default:
        value = null;
        break;
    }

    boolean matches =
        value != null
            && criterion.getValues().stream()
                .anyMatch(
                    candidate ->
                        Condition.CONTAIN.equals(criterion.getCondition())
                            ? value.contains(candidate)
                            : value.equals(candidate));
    return matches != Boolean.TRUE.equals(criterion.isNegated());
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
    AggregationMetadata entityTypeAgg = constructAggMetadata("Type", "entity");
    AggregationMetadata platformTypeAgg = constructAggMetadata("Platform", "platform");
    AggregationMetadata environmentAgg = constructAggMetadata("Environment", "origin");

    Map<String, Long> entityTypeAggregations = new HashMap<>();
    Map<String, Long> platformTypeAggregations = new HashMap<>();
    Map<String, Long> environmentAggregations = new HashMap<>();

    AggregationMetadataArray aggregationMetadataArray = new AggregationMetadataArray();

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
    finalResult.setEntities(lineageSearchEntityArray);
    finalResult.getMetadata().setAggregations(aggregationMetadataArray);
    finalResult.setNumEntities(start);
    return finalResult.setFrom(from).setPageSize(size);
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
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityType)) {
      return fromParent(entityUrn, this::getPlatform);
    }
    String platform = null;
    if (PLATFORM_ENTITY_TYPES.contains(entityType)) {
      if (DATA_JOB_ENTITY_NAME.equals(entityType)) {
        platform = UrnUtils.getUrn(entityUrn.getEntityKey().get(0)).getEntityKey().get(0);
      } else {
        platform = entityUrn.getEntityKey().get(0);
      }
    }
    if ((platform != null) && (!platform.startsWith("urn:li:dataPlatform"))) {
      platform = "urn:li:dataPlatform:" + platform;
    }

    return platform;
  }

  @VisibleForTesting
  @Nullable
  String getEnvironment(String entityType, Urn entityUrn) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityType)) {
      return fromParent(entityUrn, this::getEnvironment);
    }
    return DATASET_ENTITY_NAME.equals(entityType) ? entityUrn.getEntityKey().get(2) : null;
  }

  /**
   * A schema field carries neither a platform nor an environment of its own; it takes its parent's,
   * which is nested inside its own urn.
   */
  @Nullable
  private String fromParent(Urn schemaFieldUrn, BiFunction<String, Urn, String> ofParent) {
    return SchemaFieldUtils.parseSchemaFieldUrn(schemaFieldUrn)
        .map(parsed -> ofParent.apply(parsed.getFirst().getEntityType(), parsed.getFirst()))
        .orElse(null);
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
    if (urn.getEntityType().equals(Constants.SCHEMA_FIELD_ENTITY_NAME)) {
      try {
        // Get the dataset urn referenced inside the schemaField urn
        return Urn.createFromString(urn.getId());
      } catch (Exception e) {
        log.error("Invalid destination urn: {}", urn.getId(), e);
      }
    }
    return urn;
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
    if (inputFilters != null && !CollectionUtils.isEmpty(inputFilters.getOr())) {
      ConjunctiveCriterion conjunctiveCriterion = inputFilters.getOr().get(0);
      if (conjunctiveCriterion.hasAnd()) {
        List<String> degreeFilter =
            conjunctiveCriterion.getAnd().stream()
                .filter(criterion -> criterion.getField().equals(DEGREE_FILTER))
                .flatMap(c -> c.getValues().stream())
                .collect(Collectors.toList());
        if (!degreeFilter.isEmpty()) {
          Predicate<Integer> degreePredicate = convertFilterToPredicate(degreeFilter);
          return relationshipsFilteredByEntities
              .filter(relationship -> degreePredicate.test(relationship.getDegree()))
              .collect(Collectors.toList());
        }
      }
    }
    return relationshipsFilteredByEntities.collect(Collectors.toList());
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
   * @param maxHops the maximum number of hops away to search for. If null, defaults to 1000
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
    try (CascadeOperationContext cascade =
        CascadeOperationContext.begin(
            metricUtils, "scrollAcrossLineage", sourceUrn, -1, "datahub.lineage")) {
      rejectUnsupportedScrollFlags(opContext.getSearchContext().getLineageFlags());

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
      CachedEntityLineageResult cachedLineageResult =
          enableCache(opContext.getSearchContext().getSearchFlags())
              ? cache.get(cacheKey, CachedEntityLineageResult.class)
              : null;
      EntityLineageResult lineageResult;
      if (cachedLineageResult == null) {
        maxHops = maxHops != null ? maxHops : 1000;
        lineageResult = getLineageResult(opContext, sourceUrn, direction, maxHops);
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

      // set schemaField relationship entity to be its reference urn
      LineageRelationshipArray updatedRelationships =
          convertSchemaFieldRelationships(lineageResult);
      lineageResult.setRelationships(updatedRelationships);

      // Filter hopped result based on the set of entities to return and inputFilters before sending
      // to search
      List<LineageRelationship> lineageRelationships =
          filterRelationships(lineageResult, new HashSet<>(entities), inputFilters);
      cascade.recordEntitiesProcessed(lineageRelationships.size());

      Filter reducedFilters =
          SearchUtils.removeCriteria(
              inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));
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
      if (lineageResult.hasPartial()) {
        scrollResult.setIsPartial(lineageResult.isPartial());
      }
      return scrollResult;
    } // end try-with-resources CascadeOperationContext
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
