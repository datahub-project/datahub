package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * Base interface for defining a candidate source for recommendation module
 */
public interface RecommendationSourceWithOffline extends RecommendationSource {

  EntityService getEntityService();

  /**
   * Whether or not to fetch recommendations from offline source
   */
  boolean shouldFetchFromOffline();

  /**
   * Get the recommendation module urn given the input userUrn and request context to be used when fetching recommendation module offline
   */
  Urn getRecommendationModuleUrn(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext);

  /**
   * Get the full recommendations module itself provided the request context.
   *
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @return list of recommendation candidates
   */
  @Override
  default Optional<RecommendationModule> getRecommendationModule(@Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext) {
    if (!isEligible(userUrn, requestContext)) {
      return Optional.empty();
    }

    // If fetch offline is set, fetch the module based on the returned urn
    if (shouldFetchFromOffline()) {
      EnvelopedAspect moduleAspect;
      try {
        moduleAspect = getEntityService().getLatestEnvelopedAspect(Constants.RECOMMENDATION_MODULE_ENTITY_NAME,
            getRecommendationModuleUrn(userUrn, requestContext), Constants.RECOMMENDATION_MODULE_ASPECT_NAME);
      } catch (Exception e) {
        return Optional.empty();
      }

      if (moduleAspect == null) {
        return Optional.empty();
      } else {
        return Optional.of(new RecommendationModule(moduleAspect.getValue().data()));
      }
    }

    return RecommendationSource.super.getRecommendationModule(userUrn, requestContext);
  }
}
