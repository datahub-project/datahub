package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationContentArray;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

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
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return whether this source is eligible
   */
  boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext);

  /**
   * Get recommended items (candidates / content) provided the context
   *
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return list of recommendation candidates
   */
  @WithSpan
  List<RecommendationContent> getRecommendations(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext);

  /**
   * Get the full recommendations module itself provided the request context.
   *
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return list of recommendation candidates
   */
  default Optional<RecommendationModule> getRecommendationModule(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    if (!isEligible(userUrn, requestContext)) {
      return Optional.empty();
    }

    List<RecommendationContent> recommendations = getRecommendations(userUrn, requestContext);
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
}
