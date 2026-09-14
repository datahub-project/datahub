package com.linkedin.datahub.graphql.types.metric.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metric.DialectExpression;
import com.linkedin.metric.DialectExpressionArray;
import com.linkedin.metric.MetricExpression;
import org.testng.annotations.Test;

public class MetricExpressionMapperTest {

  @Test
  public void testMapNull() {
    assertNull(MetricExpressionMapper.map(null));
  }

  @Test
  public void testMapNoDialects() {
    MetricExpression pdl = new MetricExpression();
    pdl.setDialects(new DialectExpressionArray());

    com.linkedin.datahub.graphql.generated.MetricExpression result =
        MetricExpressionMapper.map(pdl);

    assertNotNull(result);
    assertNotNull(result.getDialects());
    assertTrue(result.getDialects().isEmpty());
  }

  @Test
  public void testMapAllDialectEnumValues() {
    for (com.linkedin.metric.Dialect pdlDialect : com.linkedin.metric.Dialect.values()) {
      // $UNKNOWN is a Rest.li synthetic sentinel, not a real dialect — it maps to OTHER
      if (pdlDialect.name().startsWith("$")) {
        continue;
      }

      DialectExpression de = new DialectExpression();
      de.setDialect(pdlDialect);
      de.setExpression("expr");

      MetricExpression pdl = new MetricExpression();
      pdl.setDialects(new DialectExpressionArray(de));

      com.linkedin.datahub.graphql.generated.MetricExpression result =
          MetricExpressionMapper.map(pdl);

      assertNotNull(result);
      assertEquals(result.getDialects().size(), 1);
      assertEquals(result.getDialects().get(0).getDialect().name(), pdlDialect.name());
    }
  }
}
