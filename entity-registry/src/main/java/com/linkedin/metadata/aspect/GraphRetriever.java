package com.linkedin.metadata.aspect;

import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface GraphRetriever {
  int DEFAULT_EDGE_FETCH_LIMIT = 1000;

  /**
   * Access graph edges
   *
   * @param sourceTypes
   * @param sourceEntityFilter
   * @param destinationTypes
   * @param destinationEntityFilter
   * @param relationshipTypes
   * @param relationshipFilter
   * @param sortCriteria
   * @param scrollId
   * @param count
   * @param startTimeMillis
   * @param endTimeMillis
   * @return
   */
  @Nonnull
  RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nullable List<String> sourceTypes,
      @Nonnull Filter sourceEntityFilter,
      @Nullable List<String> destinationTypes,
      @Nonnull Filter destinationEntityFilter,
      @Nonnull List<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      int count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis);
}
