package com.linkedin.metadata.recommendation.ranker;

import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SimpleRecommendationRanker implements RecommendationModuleRanker {
  @Override
  public List<RecommendationModule> rank(
      @Nonnull OperationContext opContext,
      @Nullable RecommendationRequestContext requestContext,
      @Nonnull List<RecommendationModule> candidates,
      int limit) {
    return candidates.subList(0, Math.min(candidates.size(), limit));
  }
}
