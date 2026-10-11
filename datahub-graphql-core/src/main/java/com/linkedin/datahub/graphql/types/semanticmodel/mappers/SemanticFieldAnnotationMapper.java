package com.linkedin.datahub.graphql.types.semanticmodel.mappers;

import com.linkedin.datahub.graphql.generated.Dimension;
import com.linkedin.datahub.graphql.generated.SemanticFieldAnnotation;
import com.linkedin.datahub.graphql.generated.SemanticFieldType;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.datahub.graphql.types.metric.mappers.MetricExpressionMapper;
import javax.annotation.Nonnull;

/**
 * Maps {@link com.linkedin.semanticmodel.SemanticFieldAnnotation} Pegasus aspects (attached to a
 * {@code schemaField} entity) to the generated GraphQL {@link SemanticFieldAnnotation}.
 */
public class SemanticFieldAnnotationMapper {

  private SemanticFieldAnnotationMapper() {}

  @Nonnull
  public static SemanticFieldAnnotation map(
      @Nonnull final com.linkedin.semanticmodel.SemanticFieldAnnotation pdl) {
    final SemanticFieldAnnotation result = new SemanticFieldAnnotation();

    if (pdl.hasType() && pdl.getType() != null) {
      result.setType(mapSemanticFieldType(pdl.getType()));
    } else {
      result.setType(SemanticFieldType.OTHER);
    }

    if (pdl.hasExpression() && pdl.getExpression() != null) {
      result.setExpression(MetricExpressionMapper.map(pdl.getExpression()));
    }

    if (pdl.hasAggregationFunction() && pdl.getAggregationFunction() != null) {
      result.setAggregationFunction(pdl.getAggregationFunction());
    }

    if (pdl.hasDimension() && pdl.getDimension() != null) {
      result.setDimension(mapDimension(pdl.getDimension()));
    }

    return result;
  }

  private static SemanticFieldType mapSemanticFieldType(
      @Nonnull com.linkedin.semanticmodel.SemanticFieldType pdlType) {
    return PdlEnumMapper.map(SemanticFieldType.class, pdlType, SemanticFieldType.OTHER);
  }

  private static Dimension mapDimension(@Nonnull com.linkedin.semanticmodel.Dimension pdl) {
    final Dimension result = new Dimension();
    result.setIsTime(pdl.isIsTime());
    return result;
  }
}
