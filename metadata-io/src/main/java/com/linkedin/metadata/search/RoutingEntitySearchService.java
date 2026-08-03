package com.linkedin.metadata.search;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.explain.ExplainResponse;

/**
 * Routes keyword entity search <strong>reads</strong> to {@link PostgresEntitySearchService} and
 * all index <strong>writes</strong> / lifecycle operations to {@link ElasticSearchService}.
 */
@RequiredArgsConstructor
public class RoutingEntitySearchService implements EntitySearchService {

  @Nonnull private final ElasticSearchService elasticSearchService;
  @Nonnull private final PostgresEntitySearchService postgresEntitySearchService;
  @Nonnull private final SearchServiceConfiguration searchServiceConfiguration;

  @Override
  public SearchServiceConfiguration getSearchServiceConfig() {
    return searchServiceConfiguration;
  }

  @Override
  public void configure() {
    elasticSearchService.configure();
  }

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    elasticSearchService.clear(opContext);
  }

  @Override
  public long docCount(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    return postgresEntitySearchService.docCount(opContext, entityName, filter);
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String document,
      @Nonnull String docId) {
    elasticSearchService.upsertDocument(opContext, entityName, document, docId);
  }

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String docId) {
    elasticSearchService.deleteDocument(opContext, entityName, docId);
  }

  @Override
  public void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
    elasticSearchService.appendRunId(opContext, urn, runId);
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
    return postgresEntitySearchService.search(
        opContext, entityNames, input, postFilters, sortCriteria, from, size);
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
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return postgresEntitySearchService.search(
        opContext, entityNames, input, postFilters, sortCriteria, from, size, facets);
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
    return postgresEntitySearchService.filter(
        opContext, entityName, filters, sortCriteria, from, size);
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
    return postgresEntitySearchService.autoComplete(
        opContext, entityName, query, field, requestParams, limit);
  }

  @Nonnull
  @Override
  public Map<String, Long> aggregateByValue(
      @Nonnull OperationContext opContext,
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    return postgresEntitySearchService.aggregateByValue(
        opContext, entityNames, field, requestParams, limit);
  }

  @Nonnull
  @Override
  public BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter requestParams,
      int from,
      @Nullable Integer size) {
    return postgresEntitySearchService.browse(
        opContext, entityName, path, requestParams, from, size);
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
    return postgresEntitySearchService.browseV2(
        opContext, entityName, path, filter, input, start, count);
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
    return postgresEntitySearchService.browseV2(
        opContext, entityNames, path, filter, input, start, count);
  }

  @Nonnull
  @Override
  public List<String> getBrowsePaths(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull Urn urn) {
    return postgresEntitySearchService.getBrowsePaths(opContext, entityName, urn);
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
    return postgresEntitySearchService.fullTextScroll(
        opContext, entities, input, postFilters, sortCriteria, scrollId, keepAlive, size, facets);
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
    return postgresEntitySearchService.structuredScroll(
        opContext, entities, input, postFilters, sortCriteria, scrollId, keepAlive, size, facets);
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
    return postgresEntitySearchService.explain(
        opContext,
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

  @Nonnull
  @Override
  public Map<Urn, Map<String, Object>> raw(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> urns) {
    return postgresEntitySearchService.raw(opContext, urns);
  }

  @Override
  public boolean validateAndSwapAlias(
      @Nonnull OperationContext opContext,
      @Nonnull String aliasName,
      @Nonnull String newBackingIndex)
      throws Exception {
    return elasticSearchService.validateAndSwapAlias(opContext, aliasName, newBackingIndex);
  }
}
