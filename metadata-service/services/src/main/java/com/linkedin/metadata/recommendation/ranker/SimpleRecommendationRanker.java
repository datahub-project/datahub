/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
