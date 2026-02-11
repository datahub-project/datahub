package com.linkedin.metadata.search.rescore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.search.SearchHit;
import org.testng.annotations.Test;

public class Exp4jRescorerTest {

  @Test
  public void testBasicFormula() {
    // Create a simple formula with two signals
    String formula = "pow(norm_bm25, 1.0) * pow(norm_views, 0.5)";

    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("bm25")
                .normalizedName("norm_bm25")
                .fieldPath("_score")
                .type(SignalType.SCORE)
                .normalization(NormalizationConfig.sigmoid(500.0, 1.0, 2.0))
                .boost(1.0)
                .build(),
            SignalDefinition.builder()
                .name("viewCount")
                .normalizedName("norm_views")
                .fieldPath("viewCount")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.sigmoid(1000.0, 1.0, 2.0))
                .boost(0.5)
                .build());

    Exp4jRescorer rescorer = new Exp4jRescorer(formula, signals);

    assertEquals(rescorer.getFormula(), formula);
    assertEquals(rescorer.getSignals().size(), 2);
  }

  @Test
  public void testFormulaWithBooleanSignals() {
    // Test formula with boolean signals
    String formula = "pow(hasDesc, 1.3) * pow(hasOwners, 1.2)";

    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("hasDescription")
                .normalizedName("hasDesc")
                .fieldPath("hasDescription")
                .type(SignalType.BOOLEAN)
                .normalization(NormalizationConfig.bool(1.3, 1.0))
                .boost(1.3)
                .build(),
            SignalDefinition.builder()
                .name("hasOwners")
                .normalizedName("hasOwners")
                .fieldPath("hasOwners")
                .type(SignalType.BOOLEAN)
                .normalization(NormalizationConfig.bool(1.2, 1.0))
                .boost(1.2)
                .build());

    Exp4jRescorer rescorer = new Exp4jRescorer(formula, signals);

    assertEquals(rescorer.getFormula(), formula);
    assertEquals(rescorer.getSignals().size(), 2);
  }

  @Test
  public void testRescorerWithMockHit() {
    // Create a simple formula
    String formula = "pow(norm_bm25, 1.0) * pow(hasDesc, 1.0)";

    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("bm25")
                .normalizedName("norm_bm25")
                .fieldPath("_score")
                .type(SignalType.SCORE)
                .normalization(NormalizationConfig.sigmoid(500.0, 1.0, 2.0))
                .boost(1.0)
                .build(),
            SignalDefinition.builder()
                .name("hasDescription")
                .normalizedName("hasDesc")
                .fieldPath("hasDescription")
                .type(SignalType.BOOLEAN)
                .normalization(NormalizationConfig.bool(1.3, 1.0))
                .boost(1.0)
                .build());

    Exp4jRescorer rescorer = new Exp4jRescorer(formula, signals);

    // Create a mock SearchHit
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("hasDescription", true);
    sourceMap.put("viewCount", 500);

    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(100.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    RescoreResult result = rescorer.rescore(hit, 100.0);

    assertNotNull(result);
    assertEquals(result.getDocumentId(), "doc1");
    assertEquals(result.getBm25Score(), 100.0);
    assertTrue(result.getFinalScore() > 0);
    assertNotNull(result.getSignals());
    assertEquals(result.getSignals().size(), 2);

    // Verify signal values
    SignalValue bm25Signal = result.getSignals().get("bm25");
    assertNotNull(bm25Signal);
    assertEquals(bm25Signal.getName(), "bm25");
    assertEquals(bm25Signal.getRawValue(), 100.0);
    assertTrue(bm25Signal.getNormalizedValue() >= 1.0 && bm25Signal.getNormalizedValue() <= 2.0);

    SignalValue hasDescSignal = result.getSignals().get("hasDescription");
    assertNotNull(hasDescSignal);
    assertEquals(hasDescSignal.getName(), "hasDescription");
    assertEquals(hasDescSignal.getRawValue(), true);
    assertEquals(hasDescSignal.getNormalizedValue(), 1.3); // trueValue
  }

  @Test
  public void testRescorerWithMissingFields() {
    // Test behavior when document is missing some fields
    String formula = "pow(norm_bm25, 1.0) * pow(norm_views, 0.5)";

    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("bm25")
                .normalizedName("norm_bm25")
                .fieldPath("_score")
                .type(SignalType.SCORE)
                .normalization(NormalizationConfig.sigmoid(500.0, 1.0, 2.0))
                .boost(1.0)
                .build(),
            SignalDefinition.builder()
                .name("viewCount")
                .normalizedName("norm_views")
                .fieldPath("viewCount")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.sigmoid(1000.0, 1.0, 2.0))
                .boost(0.5)
                .build());

    Exp4jRescorer rescorer = new Exp4jRescorer(formula, signals);

    // Create a mock SearchHit without viewCount
    Map<String, Object> sourceMap = new HashMap<>();

    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc2");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    RescoreResult result = rescorer.rescore(hit, 50.0);

    assertNotNull(result);
    assertTrue(result.getFinalScore() > 0);

    // viewCount should have null raw value (missing) which normalizes to outputMin
    SignalValue viewsSignal = result.getSignals().get("viewCount");
    assertNotNull(viewsSignal);
    assertNull(viewsSignal.getRawValue());
  }

  @Test
  public void testNaNResultReturnsNeutralValue() {
    // Formula that can produce NaN: 0/0
    // Note: We need to use variables that are set to 0 to get 0/0
    String formula = "norm_val / norm_val - norm_val / norm_val";

    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("testSignal")
                .normalizedName("norm_val")
                .fieldPath("testField")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.none())
                .boost(1.0)
                .build());

    Exp4jRescorer rescorer = new Exp4jRescorer(formula, signals);

    // Create mock hit with field value 0, which will produce 0/0 - 0/0 = NaN
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("testField", 0);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(100.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    RescoreResult result = rescorer.rescore(hit, 100.0);

    // NaN should be converted to neutral value 1.0
    assertEquals(result.getFinalScore(), 1.0, 0.001);
  }
}
