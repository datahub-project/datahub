package com.linkedin.metadata.recommendation.ranker;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import java.util.List;
import javax.annotation.Nonnull;

public interface RecommendationModuleRanker {
  /**
   * Rank and return the final list of modules
   *
   * @param candidates Candidate modules to rank
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @param limit Max number of modules to return
   * @return ranked list of modules
   */
  List<RecommendationModule> rank(
      @Nonnull List<RecommendationModule> candidates,
      @Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext,
      int limit);
}
