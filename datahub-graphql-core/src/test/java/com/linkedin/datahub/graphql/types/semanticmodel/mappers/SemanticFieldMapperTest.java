package com.linkedin.datahub.graphql.types.semanticmodel.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.SemanticField;
import com.linkedin.datahub.graphql.generated.SemanticFieldType;
import com.linkedin.metric.AiContext;
import com.linkedin.metric.DialectExpression;
import com.linkedin.metric.DialectExpressionArray;
import com.linkedin.metric.MetricExpression;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import com.linkedin.semanticmodel.Dimension;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class SemanticFieldMapperTest {

  private static final String SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.orders_model,my_model)";

  @Test
  public void testMapFullyPopulated() throws URISyntaxException {
    Urn semanticModelUrn = Urn.createFromString(SEMANTIC_MODEL_URN);

    com.linkedin.semanticmodel.SemanticField pdl =
        new com.linkedin.semanticmodel.SemanticField()
            .setSchemaField(buildSchemaField("revenue"))
            .setType(com.linkedin.semanticmodel.SemanticFieldType.MEASURE)
            .setExpression(buildExpression())
            .setDimension(new Dimension().setIsTime(true))
            .setAiContext(new AiContext().setInstructions("Sum of order revenue."));

    SemanticField result = SemanticFieldMapper.map(null, pdl, semanticModelUrn);

    assertNotNull(result);
    assertNotNull(result.getSchemaField());
    assertEquals(result.getSchemaField().getFieldPath(), "revenue");
    assertNotNull(result.getSchemaField().getSchemaFieldEntity());
    assertEquals(
        result.getSchemaField().getSchemaFieldEntity().getUrn(),
        "urn:li:schemaField:(" + SEMANTIC_MODEL_URN + ",revenue)");
    assertEquals(result.getType(), SemanticFieldType.MEASURE);
    assertNotNull(result.getExpression());
    assertEquals(result.getExpression().getDialects().size(), 1);
    assertNotNull(result.getDimension());
    assertTrue(result.getDimension().getIsTime());
    assertNotNull(result.getAiContext());
    assertEquals(result.getAiContext().getInstructions(), "Sum of order revenue.");
  }

  @Test
  public void testMapMissingTypeDefaultsToOther() throws URISyntaxException {
    Urn semanticModelUrn = Urn.createFromString(SEMANTIC_MODEL_URN);

    com.linkedin.semanticmodel.SemanticField pdl =
        new com.linkedin.semanticmodel.SemanticField().setSchemaField(buildSchemaField("field"));

    SemanticField result = SemanticFieldMapper.map(null, pdl, semanticModelUrn);

    assertEquals(result.getType(), SemanticFieldType.OTHER);
  }

  @Test
  public void testMapMissingSchemaFieldIsNull() throws URISyntaxException {
    Urn semanticModelUrn = Urn.createFromString(SEMANTIC_MODEL_URN);

    com.linkedin.semanticmodel.SemanticField pdl =
        new com.linkedin.semanticmodel.SemanticField()
            .setType(com.linkedin.semanticmodel.SemanticFieldType.FILTER);

    SemanticField result = SemanticFieldMapper.map(null, pdl, semanticModelUrn);

    assertNull(result.getSchemaField());
    assertEquals(result.getType(), SemanticFieldType.FILTER);
  }

  private static SchemaField buildSchemaField(String fieldPath) {
    return new SchemaField()
        .setFieldPath(fieldPath)
        .setNativeDataType("string")
        .setType(
            new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
  }

  private static MetricExpression buildExpression() {
    return new MetricExpression()
        .setDialects(
            new DialectExpressionArray(
                new DialectExpression()
                    .setDialect(com.linkedin.metric.Dialect.ANSI_SQL)
                    .setExpression("SUM(revenue)")));
  }
}
