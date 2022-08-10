package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class ElasticSearchService implements EntitySearchService {

  private static final int MAX_RUN_IDS_INDEXED = 25; // Save the previous 25 run ids in the index.
  private final EntityIndexBuilders indexBuilders;
  private final ESSearchDAO esSearchDAO;
  private final ESBrowseDAO esBrowseDAO;
  private final ESWriteDAO esWriteDAO;

  @Override
  public void configure() {
    indexBuilders.buildAll();
  }

  @Override
  public void clear() {
    esWriteDAO.clear();
  }

  @Override
  public long docCount(@Nonnull String entityName) {
    return esSearchDAO.docCount(entityName);
  }

  @Override
  public void upsertDocument(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId) {
    log.debug(String.format("Upserting Search document entityName: %s, document: %s, docId: %s", entityName, document,
        docId));
    esWriteDAO.upsertDocument(entityName, document, docId);
  }

  @Override
  public void deleteDocument(@Nonnull String entityName, @Nonnull String docId) {
    log.debug(String.format("Deleting Search document entityName: %s, docId: %s", entityName, docId));
    esWriteDAO.deleteDocument(entityName, docId);
  }

  @Override
  public void appendRunId(@Nonnull String entityName, @Nonnull Urn urn, @Nullable String runId) {
    final Optional<String> maybeDocId = SearchUtils.getDocId(urn);
    if (!maybeDocId.isPresent()) {
      log.warn(String.format("Failed to append run id, could not generate a doc id for urn %s", urn));
      return;
    }
    final String docId = maybeDocId.get();
    log.debug(String.format("Appending run id for entityName: %s, docId: %s", entityName, docId));
    esWriteDAO.applyScriptUpdate(entityName, docId,
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
            runId,
            runId,
            MAX_RUN_IDS_INDEXED,
            runId));
  }

  @Nonnull
  @Override
  public SearchResult search(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    log.debug(String.format(
        "Searching Search documents entityName: %s, input: %s, postFilters: %s, sortCriterion: %s, from: %s, size: %s",
        entityName, input, postFilters, sortCriterion, from, size));
    return esSearchDAO.search(entityName, input, postFilters, sortCriterion, from, size);
  }

  @Nonnull
  @Override
  public SearchResult filter(@Nonnull String entityName, @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    log.debug(
        String.format("Filtering Search documents entityName: %s, filters: %s, sortCriterion: %s, from: %s, size: %s",
            entityName, filters, sortCriterion, from, size));
    return esSearchDAO.filter(entityName, filters, sortCriterion, from, size);
  }

  @Nonnull
  @Override
  public AutoCompleteResult autoComplete(@Nonnull String entityName, @Nonnull String query, @Nullable String field,
      @Nullable Filter requestParams, int limit) {
    log.debug(String.format("Autocompleting query entityName: %s, query: %s, field: %s, requestParams: %s, limit: %s",
        entityName, query, field, requestParams, limit));
    return esSearchDAO.autoComplete(entityName, query, field, requestParams, limit);
  }

  @Nonnull
  @Override
  public Map<String, Long> aggregateByValue(@Nullable String entityName, @Nonnull String field,
      @Nullable Filter requestParams, int limit) {
    log.debug("Aggregating by value: {}, field: {}, requestParams: {}, limit: {}", entityName, field, requestParams,
        limit);
    return esSearchDAO.aggregateByValue(entityName, field, requestParams, limit);
  }

  @Nonnull
  @Override
  public BrowseResult browse(@Nonnull String entityName, @Nonnull String path, @Nullable Filter requestParams, int from,
      int size) {
    log.debug(
        String.format("Browsing entities entityName: %s, path: %s, requestParams: %s, from: %s, size: %s", entityName,
            path, requestParams, from, size));
    return esBrowseDAO.browse(entityName, path, requestParams, from, size);
  }

  @Nonnull
  @Override
  public List<String> getBrowsePaths(@Nonnull String entityName, @Nonnull Urn urn) {
    log.debug(String.format("Getting browse paths for entity entityName: %s, urn: %s", entityName, urn));
    return esBrowseDAO.getBrowsePaths(entityName, urn);
  }

  @Override
  public int maxResultSize() {
    return ESUtils.MAX_RESULT_SIZE;
  }
}
