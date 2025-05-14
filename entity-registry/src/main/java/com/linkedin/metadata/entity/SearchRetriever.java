package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SearchRetriever {

  SearchFlags RETRIEVER_SEARCH_FLAGS =
      new SearchFlags()
          .setFulltext(false)
          .setMaxAggValues(20)
          .setSkipCache(false)
          .setSkipAggregates(true)
          .setSkipHighlighting(true)
          .setIncludeSoftDeleted(false)
          .setIncludeRestricted(false);

  SearchFlags RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS =
      new SearchFlags()
          .setFulltext(false)
          .setMaxAggValues(20)
          .setSkipCache(true)
          .setSkipAggregates(true)
          .setSkipHighlighting(true)
          .setIncludeSoftDeleted(false)
          .setIncludeRestricted(false)
          .setFilterNonLatestVersions(false);

  /**
   * Allows for configuring the sort, should only be used when sort specified is unique. More often
   * the default is desirable to just use the urnSort
   */
  ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nullable Filter filters,
      @Nullable String scrollId,
      int count,
      List<SortCriterion> sortCriteria,
      @Nullable SearchFlags searchFlags);

  /**
   * Returns search results for the given entities, filtered and sorted.
   *
   * @param entities list of entities to search
   * @param filters filters to apply
   * @param scrollId pagination token
   * @param count size of a page
   * @return result of the search
   */
  default ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nullable Filter filters,
      @Nullable String scrollId,
      int count) {
    SortCriterion urnSort = new SortCriterion();
    urnSort.setField("urn");
    urnSort.setOrder(SortOrder.ASCENDING);
    return scroll(
        entities, filters, scrollId, count, ImmutableList.of(urnSort), RETRIEVER_SEARCH_FLAGS);
  }

  SearchRetriever EMPTY = new EmptySearchRetriever();

  class EmptySearchRetriever implements SearchRetriever {

    @Override
    public ScrollResult scroll(
        @Nonnull List<String> entities,
        @Nullable Filter filters,
        @Nullable String scrollId,
        int count,
        List<SortCriterion> sortCriteria,
        @Nullable SearchFlags searchFlags) {
      ScrollResult empty = new ScrollResult();
      empty.setEntities(new SearchEntityArray());
      empty.setNumEntities(0);
      empty.setPageSize(0);
      return empty;
    }
  }
}
