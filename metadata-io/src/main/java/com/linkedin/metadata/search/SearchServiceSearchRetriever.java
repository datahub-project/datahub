package com.linkedin.metadata.search;

import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class SearchServiceSearchRetriever implements SearchRetriever {
  private static final SearchFlags RETRIEVER_SEARCH_FLAGS =
      new SearchFlags()
          .setFulltext(false)
          .setMaxAggValues(20)
          .setSkipCache(false)
          .setSkipAggregates(true)
          .setSkipHighlighting(true)
          .setIncludeSoftDeleted(false)
          .setIncludeRestricted(false);

  @Setter private OperationContext systemOperationContext;
  private final SearchService searchService;

  @Override
  public ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nullable Filter filters,
      @Nullable String scrollId,
      int count,
      List<SortCriterion> sortCriteria) {
    List<SortCriterion> finalCriteria = new ArrayList<>(sortCriteria);
    if (sortCriteria.stream().noneMatch(sortCriterion -> "urn".equals(sortCriterion.getField()))) {
      SortCriterion urnSort = new SortCriterion();
      urnSort.setField("urn");
      urnSort.setOrder(SortOrder.ASCENDING);
      finalCriteria.add(urnSort);
    }
    return searchService.scrollAcrossEntities(
        systemOperationContext.withSearchFlags(flags -> RETRIEVER_SEARCH_FLAGS),
        entities,
        "*",
        filters,
        finalCriteria,
        scrollId,
        null,
        count);
  }
}
