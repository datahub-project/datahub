package com.linkedin.metadata.aspect;

import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
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

  /**
   * Consume graph edges
   *
   * @param consumer consumer function, return true to exit early
   * @param sourceTypes
   * @param sourceEntityFilter
   * @param destinationTypes
   * @param destinationEntityFilter
   * @param relationshipTypes
   * @param relationshipFilter
   * @param sortCriteria
   * @param count
   * @param startTimeMillis
   * @param endTimeMillis
   */
  default void consumeRelatedEntities(
      @Nonnull Function<RelatedEntitiesScrollResult, Boolean> consumer,
      @Nullable List<String> sourceTypes,
      @Nonnull Filter sourceEntityFilter,
      @Nullable List<String> destinationTypes,
      @Nonnull Filter destinationEntityFilter,
      @Nonnull List<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter,
      @Nonnull List<SortCriterion> sortCriteria,
      int count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {

    String scrollId = null;
    boolean exitCriteria = false;

    while (!exitCriteria) {
      RelatedEntitiesScrollResult result =
          scrollRelatedEntities(
              sourceTypes,
              sourceEntityFilter,
              destinationTypes,
              destinationEntityFilter,
              relationshipTypes,
              relationshipFilter,
              sortCriteria,
              scrollId,
              count,
              startTimeMillis,
              endTimeMillis);

      exitCriteria = consumer.apply(result);

      if (result == null || result.getEntities().isEmpty() || result.getScrollId() == null) {
        exitCriteria = true;
      } else {
        scrollId = result.getScrollId();
      }
    }
  }

  GraphRetriever EMPTY = new EmptyGraphRetriever();

  class EmptyGraphRetriever implements GraphRetriever {

    @Nonnull
    @Override
    public RelatedEntitiesScrollResult scrollRelatedEntities(
        @Nullable List<String> sourceTypes,
        @Nonnull Filter sourceEntityFilter,
        @Nullable List<String> destinationTypes,
        @Nonnull Filter destinationEntityFilter,
        @Nonnull List<String> relationshipTypes,
        @Nonnull RelationshipFilter relationshipFilter,
        @Nonnull List<SortCriterion> sortCriterion,
        @Nullable String scrollId,
        int count,
        @Nullable Long startTimeMillis,
        @Nullable Long endTimeMillis) {
      return new RelatedEntitiesScrollResult(0, 0, null, Collections.emptyList());
    }
  }
}
