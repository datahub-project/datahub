package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class ElasticSearchService implements SearchService {

  private final ESIndexBuilders indexBuilders;
  private final ESSearchDAO esSearchDAO;
  private final ESBrowseDAO esBrowseDAO;
  private final ESWriteDAO esWriteDAO;

  @Override
  public void configure() {
    indexBuilders.buildAll();
  }

  @Override
  public void upsertDocument(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId) {
    esWriteDAO.upsertDocument(entityName, document, docId);
  }

  @Override
  public void deleteDocument(@Nonnull String entityName, @Nonnull String docId) {
    esWriteDAO.deleteDocument(entityName, docId);
  }

  @Nonnull
  @Override
  public SearchResult search(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    return esSearchDAO.search(entityName, input, postFilters, sortCriterion, from, size);
  }

  @Nonnull
  @Override
  public SearchResult filter(@Nonnull String entityName, @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    return esSearchDAO.filter(entityName, filters, sortCriterion, from, size);
  }

  @Nonnull
  @Override
  public AutoCompleteResult autoComplete(@Nonnull String entityName, @Nonnull String query, @Nullable String field,
      @Nullable Filter requestParams, int limit) {
    return esSearchDAO.autoComplete(entityName, query, field, requestParams, limit);
  }

  @Nonnull
  @Override
  public BrowseResult browse(@Nonnull String entityName, @Nonnull String path, @Nullable Filter requestParams, int from,
      int size) {
    return esBrowseDAO.browse(entityName, path, requestParams, from, size);
  }

  @Nonnull
  @Override
  public List<String> getBrowsePaths(@Nonnull String entityName, @Nonnull Urn urn) {
    return esBrowseDAO.getBrowsePaths(entityName, urn);
  }
}
