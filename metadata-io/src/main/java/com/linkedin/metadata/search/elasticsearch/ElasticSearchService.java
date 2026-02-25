package com.linkedin.metadata.search.elasticsearch;

import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.*;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.search.SearchResponse;

@Slf4j
@RequiredArgsConstructor
public class ElasticSearchService implements EntitySearchService, ElasticSearchIndexed {
  private final ESIndexBuilder indexBuilder;
  @Getter private final SearchServiceConfiguration searchServiceConfig;
  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final MappingsBuilder mappingsBuilder;
  private final SettingsBuilder settingsBuilder;

  public static final SearchFlags DEFAULT_SERVICE_SEARCH_FLAGS =
      new SearchFlags()
          .setFulltext(false)
          .setMaxAggValues(20)
          .setSkipCache(false)
          .setSkipAggregates(false)
          .setSkipHighlighting(false)
          .setIncludeSoftDeleted(false)
          .setIncludeRestricted(false);

  private static final int MAX_RUN_IDS_INDEXED = 25; // Save the previous 25 run ids in the index.
  public static final String SCRIPT_SOURCE =
      "if (ctx._source.containsKey('runId')) { "
          + "if (!ctx._source.runId.contains(params.runId)) { "
          + "ctx._source.runId.add(params.runId); "
          + "if (ctx._source.runId.length > params.maxRunIds) { ctx._source.runId.remove(0) } } "
          + "} else { ctx._source.runId = [params.runId] }";

  @VisibleForTesting @Getter private final ESSearchDAO esSearchDAO;
  private final ESBrowseDAO esBrowseDAO;
  @Getter private final ESWriteDAO esWriteDAO;

  @Override
  public void reindexAll(
      @Nonnull OperationContext opContext,
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    for (ReindexConfig config : buildReindexConfigs(opContext, properties)) {
      try {
        indexBuilder.buildIndex(config);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      @Nonnull OperationContext opContext,
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {

    return indexBuilder.buildReindexConfigs(
        opContext, settingsBuilder, mappingsBuilder, properties);
  }

  /**
   * Given a structured property generate all entity index configurations impacted by it, preserving
   * existing properties
   *
   * @param property the new property
   * @return index configurations impacted by the new property
   */
  public List<ReindexConfig> buildReindexConfigsWithNewStructProp(
      @Nonnull OperationContext opContext, Urn urn, StructuredPropertyDefinition property) {

    return indexBuilder.buildReindexConfigsWithNewStructProp(
        opContext, settingsBuilder, mappingsBuilder, urn, property);
  }

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    Set<String> deletedIndexNames = esWriteDAO.clear(opContext);

    // Recreate the indices that were deleted
    if (!deletedIndexNames.isEmpty()) {
      try {
        List<ReindexConfig> allConfigs =
            indexBuilder.buildReindexConfigs(
                opContext, settingsBuilder, mappingsBuilder, Collections.emptySet());

        // Filter to only recreate indices that were deleted
        for (ReindexConfig config : allConfigs) {
          if (deletedIndexNames.contains(config.name())) {
            indexBuilder.buildIndex(config);
            log.info("Recreated index {} after clearing", config.name());
          }
        }
        log.info("Recreated {} indices after clearing", deletedIndexNames.size());
      } catch (IOException e) {
        log.error("Failed to recreate indices after clearing", e);
        throw new RuntimeException("Failed to recreate indices after clearing", e);
      }
    }
  }

  @Override
  public long docCount(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    return esSearchDAO.docCount(
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)),
        entityName,
        filter);
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String document,
      @Nonnull String docId) {
    log.debug(
        String.format(
            "Upserting Search document entityName: %s, document: %s, docId: %s",
            entityName, document, docId));
    esWriteDAO.upsertDocument(opContext, entityName, document, docId);
  }

  /**
   * Updates or inserts the given search document in the specified index. This method works directly
   * with index names, useful for V3 multi-entity indices.
   *
   * @param indexName name of the index
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocumentByIndexName(
      @Nonnull String indexName, @Nonnull String document, @Nonnull String docId) {
    log.debug(
        String.format(
            "Upserting Search document indexName: %s, document: %s, docId: %s",
            indexName, document, docId));
    esWriteDAO.upsertDocumentByIndexName(indexName, document, docId);
  }

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String docId) {
    log.debug(
        String.format("Deleting Search document entityName: %s, docId: %s", entityName, docId));
    esWriteDAO.deleteDocument(opContext, entityName, docId);
  }

  /**
   * Deletes the document with the given document ID from the specified index. This method works
   * directly with index names, useful for V3 multi-entity indices.
   *
   * @param indexName name of the index
   * @param docId the ID of the document to delete
   */
  public void deleteDocumentByIndexName(@Nonnull String indexName, @Nonnull String docId) {
    log.debug(String.format("Deleting Search document indexName: %s, docId: %s", indexName, docId));
    esWriteDAO.deleteDocumentByIndexName(indexName, docId);
  }

  /**
   * Checks if the given index exists in OpenSearch.
   *
   * @param indexName name of the index to check
   * @return true if the index exists, false otherwise
   */
  public boolean indexExists(@Nonnull String indexName) {
    return esWriteDAO.indexExists(indexName);
  }

  /**
   * Applies a script update to a document in a specific index. This method works directly with
   * index names, useful for applying script updates to semantic indices.
   *
   * @param indexName the name of the index
   * @param docId the document ID
   * @param scriptSource the script source code
   * @param scriptParams the script parameters
   * @param upsert the document to upsert if it doesn't exist
   */
  public void applyScriptUpdateByIndexName(
      @Nonnull String indexName,
      @Nonnull String docId,
      @Nonnull String scriptSource,
      @Nonnull Map<String, Object> scriptParams,
      Map<String, Object> upsert) {
    log.debug(
        "Applying script update to indexName: {}, docId: {}, script: {}",
        indexName,
        docId,
        scriptSource);
    esWriteDAO.applyScriptUpdateByIndexName(indexName, docId, scriptSource, scriptParams, upsert);
  }

  /**
   * Updates or inserts the given search document in the V3 index for the specified search group.
   * This method uses the index convention to properly construct the V3 index name.
   *
   * @param opContext the operation context
   * @param searchGroup the search group name
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocumentBySearchGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String searchGroup,
      @Nonnull String document,
      @Nonnull String docId) {
    log.debug(
        String.format(
            "Upserting Search document searchGroup: %s, document: %s, docId: %s",
            searchGroup, document, docId));
    esWriteDAO.upsertDocumentBySearchGroup(opContext, searchGroup, document, docId);
  }

  /**
   * Deletes the document with the given document ID from the V3 index for the specified search
   * group. This method uses the index convention to properly construct the V3 index name.
   *
   * @param opContext the operation context
   * @param searchGroup the search group name
   * @param docId the ID of the document to delete
   */
  public void deleteDocumentBySearchGroup(
      @Nonnull OperationContext opContext, @Nonnull String searchGroup, @Nonnull String docId) {
    log.debug(
        String.format("Deleting Search document searchGroup: %s, docId: %s", searchGroup, docId));
    esWriteDAO.deleteDocumentBySearchGroup(opContext, searchGroup, docId);
  }

  @Override
  public void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
    final String entityName = urn.getEntityType();
    final String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);

    log.info("Appending run id for entity '{}', docId='{}', runId='{}'", entityName, docId, runId);

    // Create an upsert document that will be used if the document doesn't exist
    Map<String, Object> upsert = new HashMap<>();
    upsert.put("urn", urn.toString());
    upsert.put("runId", Collections.singletonList(runId));

    Map<String, Object> scriptParams = new HashMap<>();
    scriptParams.put("runId", runId);
    scriptParams.put("maxRunIds", MAX_RUN_IDS_INDEXED);

    // Update V2 index
    esWriteDAO.applyScriptUpdate(
        opContext,
        entityName,
        docId,
        /*
          Parameterized script used to apply updates to the runId field of the index.
          This script saves the past N run ids which touched a particular URN in the search index.
          It only adds a new run id if it is not already stored inside the list. (List is unique AND ordered)
        */
        SCRIPT_SOURCE,
        scriptParams,
        upsert);

    // Dual-write to semantic index if it exists
    String semanticIndexName =
        opContext.getSearchContext().getIndexConvention().getEntityIndexNameSemantic(entityName);
    if (indexExists(semanticIndexName)) {
      log.info(
          "Semantic dual-write: APPEND_RUNID to '{}' for entity '{}', docId='{}', runId='{}'",
          semanticIndexName,
          entityName,
          docId,
          runId);
      applyScriptUpdateByIndexName(semanticIndexName, docId, SCRIPT_SOURCE, scriptParams, upsert);
    } else {
      log.info(
          "Semantic dual-write: SKIP - index '{}' does not exist for runId update",
          semanticIndexName);
    }
  }

  @Nonnull
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    return search(opContext, entityNames, input, postFilters, sortCriteria, from, size, List.of());
  }

  @Nonnull
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    log.debug(
        String.format(
            "Searching FullText Search documents entityName: %s, input: %s, postFilters: %s, sortCriteria: %s, from: %s, size: %s",
            entityNames, input, postFilters, sortCriteria, from, size));

    return esSearchDAO.search(
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, input, DEFAULT_SERVICE_SEARCH_FLAGS)),
        entityNames,
        input,
        postFilters,
        sortCriteria,
        from,
        size,
        facets);
  }

  @Nonnull
  @Override
  public SearchResult filter(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    log.debug(
        String.format(
            "Filtering Search documents entityName: %s, filters: %s, sortCriteria: %s, from: %s, size: %s",
            entityName, filters, sortCriteria, from, size));

    return esSearchDAO.filter(
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)),
        entityName,
        filters,
        sortCriteria,
        from,
        size);
  }

  @Nonnull
  @Override
  public AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    log.debug(
        String.format(
            "Autocompleting query entityName: %s, query: %s, field: %s, requestParams: %s, limit: %s",
            entityName, query, field, requestParams, limit));

    return esSearchDAO.autoComplete(
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, query, DEFAULT_SERVICE_SEARCH_FLAGS)),
        entityName,
        query,
        field,
        requestParams,
        limit);
  }

  @Nonnull
  @Override
  public Map<String, Long> aggregateByValue(
      @Nonnull OperationContext opContext,
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    log.debug(
        "Aggregating by value: {}, field: {}, requestParams: {}, limit: {}",
        entityNames != null ? entityNames.toString() : null,
        field,
        requestParams,
        limit);

    return esSearchDAO.aggregateByValue(
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)),
        entityNames,
        field,
        requestParams,
        limit);
  }

  @Nonnull
  @Override
  public BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      @Nullable Integer size) {
    log.debug(
        String.format(
            "Browsing entities entityName: %s, path: %s, filters: %s, from: %s, size: %s",
            entityName, path, filters, from, size));
    return esBrowseDAO.browse(
        opContext.withSearchFlags(
            flags ->
                applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)
                    .setFulltext(true)),
        entityName,
        path,
        filters,
        from,
        size);
  }

  @Nonnull
  @Override
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      @Nullable Integer count) {

    return esBrowseDAO.browseV2(
        opContext.withSearchFlags(
            flags ->
                applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)
                    .setFulltext(true)),
        entityName,
        path,
        filter,
        input,
        start,
        ConfigUtils.applyLimit(searchServiceConfig, count));
  }

  @Nonnull
  @Override
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      @Nullable Integer count) {

    return esBrowseDAO.browseV2(
        opContext.withSearchFlags(
            flags ->
                applyDefaultSearchFlags(flags, input, DEFAULT_SERVICE_SEARCH_FLAGS)
                    .setFulltext(true)),
        entityNames,
        path,
        filter,
        input,
        start,
        ConfigUtils.applyLimit(searchServiceConfig, count));
  }

  @Nonnull
  @Override
  public List<String> getBrowsePaths(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull Urn urn) {
    log.debug(
        String.format("Getting browse paths for entity entityName: %s, urn: %s", entityName, urn));
    return esBrowseDAO.getBrowsePaths(opContext, entityName, urn);
  }

  @Nonnull
  @Override
  public ScrollResult fullTextScroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    log.debug(
        String.format(
            "Scrolling Structured Search documents entities: %s, input: %s, postFilters: %s, sortCriteria: %s, scrollId: %s, size: %s",
            entities, input, postFilters, sortCriteria, scrollId, size));

    return esSearchDAO.scroll(
        opContext.withSearchFlags(
            flags ->
                applyDefaultSearchFlags(flags, input, DEFAULT_SERVICE_SEARCH_FLAGS)
                    .setFulltext(true)),
        entities,
        input,
        postFilters,
        sortCriteria,
        scrollId,
        keepAlive,
        size);
  }

  @Nonnull
  @Override
  public ScrollResult structuredScroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    log.debug(
        String.format(
            "Scrolling FullText Search documents entities: %s, input: %s, postFilters: %s, sortCriteria: %s, scrollId: %s, size: %s",
            entities, input, postFilters, sortCriteria, scrollId, size));

    return esSearchDAO.scroll(
        opContext.withSearchFlags(
            flags ->
                applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)
                    .setFulltext(false)),
        entities,
        input,
        postFilters,
        sortCriteria,
        scrollId,
        keepAlive,
        size);
  }

  public Optional<SearchResponse> raw(
      @Nonnull OperationContext opContext, @Nonnull String indexName, @Nullable String jsonQuery) {
    return esSearchDAO.raw(opContext, indexName, jsonQuery);
  }

  @Override
  @Nonnull
  public Map<Urn, Map<String, Object>> raw(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> urns) {
    return esSearchDAO.rawEntity(opContext, urns).entrySet().stream()
        .flatMap(
            entry ->
                Optional.ofNullable(entry.getValue().getHits().getHits())
                    .filter(hits -> hits.length > 0)
                    .map(hits -> Map.entry(entry.getKey(), hits[0]))
                    .stream())
        .map(entry -> Map.entry(entry.getKey(), entry.getValue().getSourceAsMap()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public ExplainResponse explain(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nonnull String documentId,
      @Nonnull String entityName,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {

    return esSearchDAO.explain(
        opContext.withSearchFlags(
            flags -> applyDefaultSearchFlags(flags, null, DEFAULT_SERVICE_SEARCH_FLAGS)),
        query,
        documentId,
        entityName,
        postFilters,
        sortCriteria,
        scrollId,
        keepAlive,
        size,
        facets);
  }

  @Override
  public ESIndexBuilder getIndexBuilder() {
    return indexBuilder;
  }
}
