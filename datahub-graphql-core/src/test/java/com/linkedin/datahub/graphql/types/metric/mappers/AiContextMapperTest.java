package com.linkedin.datahub.graphql.types.metric.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.data.template.StringArray;
import com.linkedin.metric.AiContext;
import org.testng.annotations.Test;

public class AiContextMapperTest {

  @Test
  public void testMapNull() {
    assertNull(AiContextMapper.map(null));
  }

  @Test
  public void testMapAllFields() {
    AiContext pdl = new AiContext();
    pdl.setSynonyms(new StringArray("rev", "revenue_total"));
    pdl.setInstructions("Use this metric for financial reporting.");
    pdl.setExamples(new StringArray("Q1 revenue = 1000000"));
    pdl.setCustomInstructions("Always filter by fiscal year.");

    com.linkedin.datahub.graphql.generated.AiContext result = AiContextMapper.map(pdl);

    assertNotNull(result);
    assertEquals(result.getSynonyms().size(), 2);
    assertEquals(result.getSynonyms().get(0), "rev");
    assertEquals(result.getSynonyms().get(1), "revenue_total");
    assertEquals(result.getInstructions(), "Use this metric for financial reporting.");
    assertEquals(result.getExamples().size(), 1);
    assertEquals(result.getExamples().get(0), "Q1 revenue = 1000000");
    assertEquals(result.getCustomInstructions(), "Always filter by fiscal year.");
  }

  @Test
  public void testMapPartialFields() {
    AiContext pdl = new AiContext();
    pdl.setInstructions("Some guidance.");

    com.linkedin.datahub.graphql.generated.AiContext result = AiContextMapper.map(pdl);

    assertNotNull(result);
    assertNull(result.getSynonyms());
    assertEquals(result.getInstructions(), "Some guidance.");
    assertNull(result.getExamples());
    assertNull(result.getCustomInstructions());
  }

  @Test
  public void testMapEmpty() {
    AiContext pdl = new AiContext();

    com.linkedin.datahub.graphql.generated.AiContext result = AiContextMapper.map(pdl);

    assertNotNull(result);
    assertNull(result.getSynonyms());
    assertNull(result.getInstructions());
    assertNull(result.getExamples());
    assertNull(result.getCustomInstructions());
  }
}
