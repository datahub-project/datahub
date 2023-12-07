package com.linkedin.metadata.recommendation.ranker;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SimpleRecommendationRanker implements RecommendationModuleRanker {
  @Override
  public List<RecommendationModule> rank(
      @Nonnull List<RecommendationModule> candidates,
      @Nonnull Urn userUrn,
      @Nullable RecommendationRequestContext requestContext,
      int limit) {
    return candidates.subList(0, Math.min(candidates.size(), limit));
  }
}
