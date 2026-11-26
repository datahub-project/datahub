package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.generated.AllowedValueInput;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.structured.PrimitivePropertyValue;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class StructuredPropertyUtilsTest {

  // Value that cannot be represented exactly in IEEE-754 float32.
  // float32(410258.29) == 410258.28125; double preserves 410258.29.
  private static final double FINANCIAL_NUMBER = 410258.29;
  private static final double FLOAT32_ROUNDED = 410258.28125;

  @Test
  public void testBindArgumentPreservesDoublePrecisionForNumberValue() {
    Map<String, Object> rawInput = new HashMap<>();
    rawInput.put("numberValue", FINANCIAL_NUMBER);

    PropertyValueInput bound = bindArgument(rawInput, PropertyValueInput.class);

    assertNotNull(bound.getNumberValue());
    assertEquals(bound.getNumberValue(), FINANCIAL_NUMBER);
    // Guard against regressing to java.lang.Float binding.
    assertTrue(Math.abs(bound.getNumberValue() - FLOAT32_ROUNDED) > 1e-6);
  }

  @Test
  public void testMapPropertyValueInputPreservesDoublePrecision() {
    Map<String, Object> rawInput = new HashMap<>();
    rawInput.put("numberValue", FINANCIAL_NUMBER);

    PropertyValueInput bound = bindArgument(rawInput, PropertyValueInput.class);
    PrimitivePropertyValue mapped = StructuredPropertyUtils.mapPropertyValueInput(bound);

    assertNotNull(mapped);
    assertTrue(mapped.isDouble());
    assertEquals(mapped.getDouble(), FINANCIAL_NUMBER);
  }

  @Test
  public void testBindArgumentPreservesDoublePrecisionForAllowedValue() {
    Map<String, Object> rawInput = new HashMap<>();
    rawInput.put("numberValue", FINANCIAL_NUMBER);
    rawInput.put("description", "gap usd");

    AllowedValueInput bound = bindArgument(rawInput, AllowedValueInput.class);

    assertNotNull(bound.getNumberValue());
    assertEquals(bound.getNumberValue(), FINANCIAL_NUMBER);
  }
}
