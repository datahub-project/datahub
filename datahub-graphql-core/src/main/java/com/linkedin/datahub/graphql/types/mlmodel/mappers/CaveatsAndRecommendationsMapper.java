/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CaveatsAndRecommendations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class CaveatsAndRecommendationsMapper
    implements ModelMapper<
        com.linkedin.ml.metadata.CaveatsAndRecommendations, CaveatsAndRecommendations> {

  public static final CaveatsAndRecommendationsMapper INSTANCE =
      new CaveatsAndRecommendationsMapper();

  public static CaveatsAndRecommendations map(
      @Nullable QueryContext context,
      @NonNull com.linkedin.ml.metadata.CaveatsAndRecommendations caveatsAndRecommendations) {
    return INSTANCE.apply(context, caveatsAndRecommendations);
  }

  @Override
  public CaveatsAndRecommendations apply(
      @Nullable QueryContext context,
      com.linkedin.ml.metadata.CaveatsAndRecommendations caveatsAndRecommendations) {
    final CaveatsAndRecommendations result = new CaveatsAndRecommendations();
    if (caveatsAndRecommendations.getCaveats() != null) {
      result.setCaveats(CaveatsDetailsMapper.map(context, caveatsAndRecommendations.getCaveats()));
    }
    if (caveatsAndRecommendations.getRecommendations() != null) {
      result.setRecommendations(caveatsAndRecommendations.getRecommendations());
    }
    if (caveatsAndRecommendations.getIdealDatasetCharacteristics() != null) {
      result.setIdealDatasetCharacteristics(
          caveatsAndRecommendations.getIdealDatasetCharacteristics());
    }
    return result;
  }
}
