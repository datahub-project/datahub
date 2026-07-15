package com.linkedin.datahub.graphql.types.semanticmodel.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dimension;
import com.linkedin.datahub.graphql.generated.SemanticField;
import com.linkedin.datahub.graphql.generated.SemanticFieldType;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaFieldMapper;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.datahub.graphql.types.metric.mappers.AiContextMapper;
import com.linkedin.datahub.graphql.types.metric.mappers.MetricExpressionMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps {@link com.linkedin.semanticmodel.SemanticField} Pegasus records to the generated GraphQL
 * {@link SemanticField}.
 *
 * <p>The {@code semanticModelUrn} parameter is threaded from the enclosing SemanticModel so that
 * the inline {@code schemaField}'s {@code SchemaFieldEntity.urn} is constructed as {@code
 * urn:li:schemaField:(<semanticModelUrn>,<fieldPath>)}.
 */
public class SemanticFieldMapper {

  private SemanticFieldMapper() {}

  @Nullable
  public static SemanticField map(
      @Nullable QueryContext context,
      @Nonnull com.linkedin.semanticmodel.SemanticField pdl,
      @Nonnull Urn semanticModelUrn) {
    final SemanticField result = new SemanticField();

    if (pdl.hasSchemaField() && pdl.getSchemaField() != null) {
      result.setSchemaField(SchemaFieldMapper.map(context, pdl.getSchemaField(), semanticModelUrn));
    }

    if (pdl.hasType() && pdl.getType() != null) {
      result.setType(mapSemanticFieldType(pdl.getType()));
    } else {
      result.setType(SemanticFieldType.OTHER);
    }

    if (pdl.hasExpression() && pdl.getExpression() != null) {
      result.setExpression(MetricExpressionMapper.map(pdl.getExpression()));
    }

    if (pdl.hasDimension() && pdl.getDimension() != null) {
      result.setDimension(mapDimension(pdl.getDimension()));
    }

    if (pdl.hasAiContext() && pdl.getAiContext() != null) {
      result.setAiContext(AiContextMapper.map(pdl.getAiContext()));
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
