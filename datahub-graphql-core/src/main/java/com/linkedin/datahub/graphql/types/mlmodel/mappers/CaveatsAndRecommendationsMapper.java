package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.CaveatsAndRecommendations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class CaveatsAndRecommendationsMapper
    implements ModelMapper<
        com.linkedin.ml.metadata.CaveatsAndRecommendations, CaveatsAndRecommendations> {

  public static final CaveatsAndRecommendationsMapper INSTANCE =
      new CaveatsAndRecommendationsMapper();

  public static CaveatsAndRecommendations map(
      @NonNull com.linkedin.ml.metadata.CaveatsAndRecommendations caveatsAndRecommendations) {
    return INSTANCE.apply(caveatsAndRecommendations);
  }

  @Override
  public CaveatsAndRecommendations apply(
      com.linkedin.ml.metadata.CaveatsAndRecommendations caveatsAndRecommendations) {
    final CaveatsAndRecommendations result = new CaveatsAndRecommendations();
    if (caveatsAndRecommendations.getCaveats() != null) {
      result.setCaveats(CaveatsDetailsMapper.map(caveatsAndRecommendations.getCaveats()));
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
