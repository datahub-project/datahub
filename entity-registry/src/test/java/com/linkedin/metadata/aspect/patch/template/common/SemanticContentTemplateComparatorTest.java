package com.linkedin.metadata.aspect.patch.template.common;

import static com.linkedin.metadata.aspect.patch.template.TemplateUtil.OBJECT_MAPPER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.Test;

public class SemanticContentTemplateComparatorTest {

  @Test
  public void testUnequalNonNumericNodesOrderInBothDirections() throws Exception {
    JsonNode abc = OBJECT_MAPPER.readTree("\"abc\"");
    JsonNode abd = OBJECT_MAPPER.readTree("\"abd\"");
    JsonNode number = OBJECT_MAPPER.readTree("1");
    JsonNode text = OBJECT_MAPPER.readTree("\"1\"");

    assertTrue(SemanticContentTemplate.NUMERIC_AWARE.compare(abc, abd) < 0);
    assertTrue(SemanticContentTemplate.NUMERIC_AWARE.compare(abd, abc) > 0);
    assertTrue(SemanticContentTemplate.NUMERIC_AWARE.compare(number, text) > 0);
    assertTrue(SemanticContentTemplate.NUMERIC_AWARE.compare(text, number) < 0);
  }

  @Test
  public void testEqualNodesCompareEqual() throws Exception {
    JsonNode left = OBJECT_MAPPER.readTree("{\"a\":[1,\"x\"]}");
    JsonNode right = OBJECT_MAPPER.readTree("{\"a\":[1,\"x\"]}");

    assertEquals(SemanticContentTemplate.NUMERIC_AWARE.compare(left, right), 0);
  }

  @Test
  public void testNumbersCompareByValue() throws Exception {
    JsonNode integer = OBJECT_MAPPER.readTree("1");
    JsonNode decimal = OBJECT_MAPPER.readTree("1.0");

    assertEquals(SemanticContentTemplate.NUMERIC_AWARE.compare(integer, decimal), 0);
  }
}
