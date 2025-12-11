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

public interface RecommendationModuleRanker {
  /**
   * Rank and return the final list of modules
   *
   * @param opContext the user's context
   * @param candidates Candidate modules to rank
   * @param requestContext Context of where the recommendations are being requested
   * @param limit Max number of modules to return
   * @return ranked list of modules
   */
  List<RecommendationModule> rank(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nonnull List<RecommendationModule> candidates,
      int limit);
}
