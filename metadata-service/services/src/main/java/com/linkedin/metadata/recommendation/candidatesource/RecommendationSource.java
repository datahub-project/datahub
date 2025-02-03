package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationContentArray;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Base interface for defining a candidate source for recommendation module */
public interface RecommendationSource {

  /** Returns the title of the module that is sourced (used in rendering) */
  String getTitle();

  /** Returns a unique module id associated with the module */
  String getModuleId();

  /** Returns the template type used for rendering recommendations from this module */
  RecommendationRenderType getRenderType();

  /**
   * Whether or not this module is eligible for resolution given the context
   *
   * @param opContext User's context requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return whether this source is eligible
   */
  boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext);

  /**
   * Get recommended items (candidates / content) provided the context
   *
   * @param opContext User's context requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return list of recommendation candidates
   */
  @WithSpan
  List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nullable Filter filter);

  // retaining this for backward compatibility
  default List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return getRecommendations(opContext, requestContext, null);
  }

  /**
   * Get the full recommendations module itself provided the request context.
   *
   * @param opContext User's context requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return list of recommendation candidates
   */
  default Optional<RecommendationModule> getRecommendationModule(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nullable Filter filter) {
    if (!isEligible(opContext, requestContext)) {
      return Optional.empty();
    }

    List<RecommendationContent> recommendations =
        getRecommendations(opContext, requestContext, filter);
    if (recommendations.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new RecommendationModule()
            .setTitle(getTitle())
            .setModuleId(getModuleId())
            .setRenderType(getRenderType())
            .setContent(new RecommendationContentArray(recommendations)));
  }

  // retaining this for backward compatibility
  default Optional<RecommendationModule> getRecommendationModule(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return getRecommendationModule(opContext, requestContext);
  }
}
