package com.linkedin.metadata.search.elasticsearch;

import static com.linkedin.metadata.search.utils.SearchUtils.applyDefaultSearchFlags;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private final EntityIndexBuilders indexBuilders;
  @VisibleForTesting @Getter private final ESSearchDAO esSearchDAO;
  private final ESBrowseDAO esBrowseDAO;
  private final ESWriteDAO esWriteDAO;

  @Override
  public void reindexAll(Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    indexBuilders.reindexAll(properties);
  }

  @Override
  public List<ReindexConfig> buildReindexConfigs(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) throws IOException {
    return indexBuilders.buildReindexConfigs(properties);
  }

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    esWriteDAO.clear(opContext);
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

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String docId) {
    log.debug(
        String.format("Deleting Search document entityName: %s, docId: %s", entityName, docId));
    esWriteDAO.deleteDocument(opContext, entityName, docId);
  }

  @Override
  public void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
    final String docId = indexBuilders.getIndexConvention().getEntityDocumentId(urn);

    log.debug(
        "Appending run id for entity name: {}, doc id: {}, run id: {}",
        urn.getEntityType(),
        docId,
        runId);
    esWriteDAO.applyScriptUpdate(
        opContext,
        urn.getEntityType(),
        docId,
        /*
          Script used to apply updates to the runId field of the index.
          This script saves the past N run ids which touched a particular URN in the search index.
          It only adds a new run id if it is not already stored inside the list. (List is unique AND ordered)
        */
        String.format(
            "if (ctx._source.containsKey('runId')) { "
                + "if (!ctx._source.runId.contains('%s')) { "
                + "ctx._source.runId.add('%s'); "
                + "if (ctx._source.runId.length > %s) { ctx._source.runId.remove(0) } } "
                + "} else { ctx._source.runId = ['%s'] }",
            runId, runId, MAX_RUN_IDS_INDEXED, runId));
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
      int size) {
    return search(opContext, entityNames, input, postFilters, sortCriteria, from, size, null);
  }

  @Nonnull
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      int size,
      @Nullable List<String> facets) {
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
      int size) {
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
      int limit) {
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
      int limit) {
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
      int size) {
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
      int count) {

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
        count);
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
      int count) {

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
        count);
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
      int size) {
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
      int size) {
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
  public int maxResultSize() {
    return ESUtils.MAX_RESULT_SIZE;
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
      int size,
      @Nullable List<String> facets) {

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
  public IndexConvention getIndexConvention() {
    return indexBuilders.getIndexConvention();
  }
}
