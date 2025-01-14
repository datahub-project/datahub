package com.linkedin.metadata.search;

import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class SearchServiceSearchRetriever implements SearchRetriever {

  @Setter private OperationContext systemOperationContext;
  private final SearchService searchService;

  @Override
  public ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nullable Filter filters,
      @Nullable String scrollId,
      int count,
      List<SortCriterion> sortCriteria,
      @Nullable SearchFlags searchFlags) {
    List<SortCriterion> finalCriteria = new ArrayList<>(sortCriteria);
    if (sortCriteria.stream().noneMatch(sortCriterion -> "urn".equals(sortCriterion.getField()))) {
      SortCriterion urnSort = new SortCriterion();
      urnSort.setField("urn");
      urnSort.setOrder(SortOrder.ASCENDING);
      finalCriteria.add(urnSort);
    }
    final SearchFlags finalSearchFlags =
        Optional.ofNullable(searchFlags).orElse(RETRIEVER_SEARCH_FLAGS);
    return searchService.scrollAcrossEntities(
        systemOperationContext.withSearchFlags(flags -> finalSearchFlags),
        entities,
        "*",
        filters,
        finalCriteria,
        scrollId,
        null,
        count);
  }
}
