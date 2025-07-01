package com.linkedin.metadata.graph;

import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Setter;

@Builder
public class SystemGraphRetriever implements GraphRetriever {
  @Setter private OperationContext systemOperationContext;
  @Nonnull private final GraphService graphService;

  @Nonnull
  @Override
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nullable Set<String> sourceTypes,
      @Nonnull Filter sourceEntityFilter,
      @Nullable Set<String> destinationTypes,
      @Nonnull Filter destinationEntityFilter,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return graphService.scrollRelatedEntities(
        systemOperationContext,
        new GraphFilters(
            sourceEntityFilter,
            destinationEntityFilter,
            sourceTypes,
            destinationTypes,
            relationshipTypes,
            relationshipFilter),
        sortCriteria,
        scrollId,
        count,
        startTimeMillis,
        endTimeMillis);
  }
}
