package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationContentArray;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * Base interface for defining a candidate source for recommendation module
 */
public interface RecommendationCandidateSource {
  // Title of the module that is sourced
  String getTitle();

  // Module ID of the module that is sourced
  String getModuleId();

  // Render type of the module that is sourced
  RecommendationRenderType getRenderType();

  /**
   * Whether or not this source is eligible given the context
   *
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return whether this source is eligible
   */
  boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext);

  /**
   * Get recommendation candidates given the context
   *
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return list of recommendation candidates
   */
  List<RecommendationContent> getCandidates(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext);

  default Optional<RecommendationModule> getModule(@Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext) {
    if (!isEligible(userUrn, requestContext)) {
      return Optional.empty();
    }

    return Optional.of(new RecommendationModule().setTitle(getTitle())
        .setModuleId(getModuleId())
        .setRenderType(getRenderType())
        .setContent(new RecommendationContentArray(getCandidates(userUrn, requestContext))));
  }
}
