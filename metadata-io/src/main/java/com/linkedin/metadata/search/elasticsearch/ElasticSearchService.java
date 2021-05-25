package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.client.RestHighLevelClient;

@RequiredArgsConstructor
public class ElasticSearchService implements SearchService {

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient searchClient;
  private final IndexConvention indexConvention;

  @Override
  public void configure() {
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs()) {
      if (entitySpec.isSearchable() || entitySpec.isBrowsable()) {
        try {
          new IndexBuilder(searchClient, entitySpec, indexConvention.getIndexName(entitySpec)).buildIndex();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public void upsert(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId) {

  }

  @Override
  public void deleteDocument(@Nonnull String docId) {

  }

  @Nonnull
  @Override
  public SearchResult search(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    return null;
  }

  @Nonnull
  @Override
  public SearchResult filter(@Nonnull String entityName, @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    return null;
  }

  @Nonnull
  @Override
  public AutoCompleteResult autoComplete(@Nonnull String entityName, @Nonnull String query, @Nullable String field,
      @Nullable Filter requestParams, int limit) {
    return null;
  }
}
