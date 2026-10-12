package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;

/**
 * Per-component bulk writer lookup. Lives in metadata-io because {@link ESBulkProcessor} does.
 * Client lookup stays on {@link SearchClusterAccess} in metadata-utils so {@code SearchContext} can
 * hold it without depending on this module.
 */
public interface SearchWriteAccess {

  @Nonnull
  ESBulkProcessor bulkProcessorFor(@Nonnull SearchComponent component);

  @Nonnull
  default ESBulkProcessor bulkProcessorForIndex(
      @Nonnull IndexConvention convention, @Nonnull String indexName) {
    SearchComponent component =
        SearchClusterAccess.tryComponentForEntityIndex(convention, indexName);
    if (component == null) {
      throw new IllegalArgumentException(
          "Index '" + indexName + "' is not a Search V2, V3, or semantic entity index");
    }
    return bulkProcessorFor(component);
  }

  @Nonnull
  static SearchWriteAccess fixed(@Nonnull ESBulkProcessor bulkProcessor) {
    return new FixedSearchWriteAccess(bulkProcessor);
  }

  @EqualsAndHashCode
  final class FixedSearchWriteAccess implements SearchWriteAccess {
    private final ESBulkProcessor bulkProcessor;

    private FixedSearchWriteAccess(@Nonnull ESBulkProcessor bulkProcessor) {
      this.bulkProcessor = bulkProcessor;
    }

    @Override
    @Nonnull
    public ESBulkProcessor bulkProcessorFor(@Nonnull SearchComponent component) {
      return bulkProcessor;
    }
  }
}
