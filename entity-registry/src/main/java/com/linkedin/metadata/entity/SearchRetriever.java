package com.linkedin.metadata.entity;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SearchRetriever {
  /**
   * Returns search results for the given entities, filtered and sorted.
   *
   * @param entities list of entities to search
   * @param filters filters to apply
   * @param scrollId pagination token
   * @param count size of a page
   * @return result of the search
   */
  ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nullable Filter filters,
      @Nullable String scrollId,
      int count);

  SearchRetriever EMPTY = new EmptySearchRetriever();

  class EmptySearchRetriever implements SearchRetriever {

    @Override
    public ScrollResult scroll(
        @Nonnull List<String> entities,
        @Nullable Filter filters,
        @Nullable String scrollId,
        int count) {
      ScrollResult empty = new ScrollResult();
      empty.setEntities(new SearchEntityArray());
      empty.setNumEntities(0);
      empty.setPageSize(0);
      return empty;
    }
  }
}
