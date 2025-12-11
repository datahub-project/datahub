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
import com.linkedin.datahub.graphql.generated.QuantitativeAnalyses;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class QuantitativeAnalysesMapper
    implements ModelMapper<com.linkedin.ml.metadata.QuantitativeAnalyses, QuantitativeAnalyses> {

  public static final QuantitativeAnalysesMapper INSTANCE = new QuantitativeAnalysesMapper();

  public static QuantitativeAnalyses map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.QuantitativeAnalyses quantitativeAnalyses) {
    return INSTANCE.apply(context, quantitativeAnalyses);
  }

  @Override
  public QuantitativeAnalyses apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.QuantitativeAnalyses quantitativeAnalyses) {
    final QuantitativeAnalyses result = new QuantitativeAnalyses();
    result.setIntersectionalResults(
        ResultsTypeMapper.map(context, quantitativeAnalyses.getIntersectionalResults()));
    result.setUnitaryResults(
        ResultsTypeMapper.map(context, quantitativeAnalyses.getUnitaryResults()));
    return result;
  }
}
