package com.linkedin.metadata.search;

import static com.datahub.authorization.AuthUtil.canViewEntity;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;
import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.query.FreshnessStats;
import com.linkedin.metadata.query.GroupingCriterion;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.cache.CachedEntityLineageResult;
import com.linkedin.metadata.search.utils.FilterUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
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

  private static final String DEGREE_FILTER = "degree";
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
  private static final int MAX_RELATIONSHIPS = 1000000;
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
      int size) {

    long startTime = System.nanoTime();
    final String finalInput = input == null || input.isEmpty() ? "*" : input;

    log.debug("Cache enabled {}, Input :{}:", cacheEnabled, finalInput);
    if (maxHops == null) {
      maxHops = 1000;
    }

    final OperationContext finalOpContext =
        opContext
            .withSearchFlags(
                flags -> applyDefaultSearchFlags(flags, finalInput, DEFAULT_SERVICE_SEARCH_FLAGS))
            .withLineageFlags(lineageFlags -> lineageFlags);

    // Cache multihop result for faster performance
    final EntityLineageResultCacheKey cacheKey =
        new EntityLineageResultCacheKey(
            finalOpContext.getSearchContextId(),
            sourceUrn,
            direction,
            maxHops,
            opContext.getSearchContext().getLineageFlags().getEntitiesExploredPerHopLimit());
    CachedEntityLineageResult cachedLineageResult = null;

    if (cacheEnabled) {
      try {
        cachedLineageResult = cache.get(cacheKey, CachedEntityLineageResult.class);
      } catch (Exception e) {
        log.warn("Failed to load cacheKey {}", cacheKey, e);
      }
    }

    EntityLineageResult lineageResult;
    FreshnessStats freshnessStats = new FreshnessStats().setCached(Boolean.FALSE);
    if (cachedLineageResult == null
        || finalOpContext.getSearchContext().getSearchFlags().isSkipCache()) {
      lineageResult =
          _graphService.getLineage(opContext, sourceUrn, direction, 0, MAX_RELATIONSHIPS, maxHops);
      if (cacheEnabled) {
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
                    _graphService.getLineage(
                        opContext, sourceUrn, direction, 0, MAX_RELATIONSHIPS, finalMaxHops);
                cache.put(cacheKey, result);
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

    String lineageGraphInfo =
        String.format(
            "Lineage Graph = time(ms):%s size:%s",
            (System.nanoTime() - startTime) / (1000.0 * 1000.0), lineageRelationships.size());
    startTime = System.nanoTime();
    long numEntities = 0;
    String codePath = null;
    try {
      Filter reducedFilters =
          SearchUtils.removeCriteria(
              inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));

      if (canDoLightning(lineageRelationships, finalInput, reducedFilters, sortCriteria)) {
        codePath = "lightning";
        // use lightning approach to return lineage search results
        LineageSearchResult lineageSearchResult =
            getLightningSearchResult(
                lineageRelationships, reducedFilters, from, size, new HashSet<>(entities));
        if (!lineageSearchResult.getEntities().isEmpty()) {
          log.debug(
              "Lightning Lineage entity result: {}",
              lineageSearchResult.getEntities().get(0).toString());
        }
        numEntities = lineageSearchResult.getNumEntities();
        return lineageSearchResult;
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
        numEntities = lineageSearchResult.getNumEntities();
        return lineageSearchResult;
      }
    } finally {
      log.info(
          "{}; Lineage Search({}) = time(ms):{} size:{}",
          lineageGraphInfo,
          codePath,
          (System.nanoTime() - startTime) / (1000.0 * 1000.0),
          numEntities);
    }
  }

  @VisibleForTesting
  boolean canDoLightning(
      List<LineageRelationship> lineageRelationships,
      String input,
      Filter inputFilters,
      List<SortCriterion> sortCriteria) {
    boolean simpleFilters =
        inputFilters == null
            || inputFilters.getOr() == null
            || inputFilters.getOr().stream()
                .allMatch(
                    criterion ->
                        criterion.getAnd().stream()
                            .allMatch(
                                criterion1 ->
                                    "platform".equals(criterion1.getField())
                                        || "origin".equals(criterion1.getField())));
    return (lineageRelationships.size()
            > appConfig.getCache().getSearch().getLineage().getLightningThreshold())
        && input.equals("*")
        && simpleFilters
        && CollectionUtils.isEmpty(sortCriteria);
  }

  @VisibleForTesting
  LineageSearchResult getLightningSearchResult(
      List<LineageRelationship> lineageRelationships,
      Filter inputFilters,
      int from,
      int size,
      Set<String> entityNames) {

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
      // Apply platform, entity types, and environment filters

      String platform = getPlatform(entityType, entityUrn);

      String environment = null;
      if (entityType.equals(DATASET_ENTITY_NAME)) {
        environment = entityUrn.getEntityKey().get(2);
      }

      // Filters
      Set<String> platformCriteriaValues = null;
      Set<String> originCriteriaValues = null;
      if (inputFilters != null && inputFilters.getOr() != null) {
        platformCriteriaValues =
            inputFilters.getOr().stream()
                .map(ConjunctiveCriterion::getAnd)
                .flatMap(CriterionArray::stream)
                .filter(criterion -> "platform".equals(criterion.getField()))
                .flatMap(criterion -> criterion.getValues().stream())
                .collect(Collectors.toSet());
        originCriteriaValues =
            inputFilters.getOr().stream()
                .map(ConjunctiveCriterion::getAnd)
                .flatMap(CriterionArray::stream)
                .filter(criterion -> "origin".equals(criterion.getField()))
                .flatMap(criterion -> criterion.getValues().stream())
                .collect(Collectors.toSet());
      }
      boolean isNotFiltered =
          (entityNames.isEmpty() || entityNames.contains(entityUrn.getEntityType()))
              && (CollectionUtils.isEmpty(platformCriteriaValues)
                  || (platform != null && platformCriteriaValues.contains(platform)))
              && (CollectionUtils.isEmpty(originCriteriaValues)
                  || (environment != null && originCriteriaValues.contains(environment)));

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
      int size) {

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
      int size) {
    // Cache multihop result for faster performance
    final EntityLineageResultCacheKey cacheKey =
        new EntityLineageResultCacheKey(
            opContext.getSearchContextId(),
            sourceUrn,
            direction,
            maxHops,
            opContext.getSearchContext().getLineageFlags().getEntitiesExploredPerHopLimit());
    CachedEntityLineageResult cachedLineageResult =
        cacheEnabled ? cache.get(cacheKey, CachedEntityLineageResult.class) : null;
    EntityLineageResult lineageResult;
    if (cachedLineageResult == null) {
      maxHops = maxHops != null ? maxHops : 1000;
      lineageResult =
          _graphService.getLineage(opContext, sourceUrn, direction, 0, MAX_RELATIONSHIPS, maxHops);
      if (cacheEnabled) {
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
    LineageRelationshipArray updatedRelationships = convertSchemaFieldRelationships(lineageResult);
    lineageResult.setRelationships(updatedRelationships);

    // Filter hopped result based on the set of entities to return and inputFilters before sending
    // to search
    List<LineageRelationship> lineageRelationships =
        filterRelationships(lineageResult, new HashSet<>(entities), inputFilters);

    Filter reducedFilters =
        SearchUtils.removeCriteria(
            inputFilters, criterion -> criterion.getField().equals(DEGREE_FILTER));
    return getScrollResultInBatches(
        opContext,
        lineageRelationships,
        input != null ? input : "*",
        reducedFilters,
        sortCriteria,
        scrollId,
        keepAlive,
        size);
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
      int size) {

    OperationContext finalOpContext =
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, input, DEFAULT_SERVICE_SEARCH_FLAGS));

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
                  querySize),
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
}
