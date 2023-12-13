package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.QuantitativeAnalyses;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class QuantitativeAnalysesMapper
    implements ModelMapper<com.linkedin.ml.metadata.QuantitativeAnalyses, QuantitativeAnalyses> {

  public static final QuantitativeAnalysesMapper INSTANCE = new QuantitativeAnalysesMapper();

  public static QuantitativeAnalyses map(
      @NonNull final com.linkedin.ml.metadata.QuantitativeAnalyses quantitativeAnalyses) {
    return INSTANCE.apply(quantitativeAnalyses);
  }

  @Override
  public QuantitativeAnalyses apply(
      @NonNull final com.linkedin.ml.metadata.QuantitativeAnalyses quantitativeAnalyses) {
    final QuantitativeAnalyses result = new QuantitativeAnalyses();
    result.setIntersectionalResults(
        ResultsTypeMapper.map(quantitativeAnalyses.getIntersectionalResults()));
    result.setUnitaryResults(ResultsTypeMapper.map(quantitativeAnalyses.getUnitaryResults()));
    return result;
  }
}
