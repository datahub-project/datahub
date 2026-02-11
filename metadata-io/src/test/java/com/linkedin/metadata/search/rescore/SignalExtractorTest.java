package com.linkedin.metadata.search.rescore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.search.SearchHit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SignalExtractorTest {

  private SignalExtractor extractor;

  @BeforeMethod
  public void setup() {
    extractor = new SignalExtractor();
  }

  @Test
  public void testExtractScoreSignal() {
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("bm25")
                .normalizedName("norm_bm25")
                .fieldPath("_score")
                .type(SignalType.SCORE)
                .normalization(NormalizationConfig.sigmoid(500.0, 1.0, 2.0))
                .boost(1.0)
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(100.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 100.0, signals);

    assertEquals(result.size(), 1);
    SignalValue bm25 = result.get("bm25");
    assertNotNull(bm25);
    assertEquals(bm25.getRawValue(), 100.0);
    assertEquals(bm25.getNumericValue(), 100.0);
    // Normalized value should be between 1.0 and 2.0
    assertTrue(bm25.getNormalizedValue() >= 1.0 && bm25.getNormalizedValue() <= 2.0);
  }

  @Test
  public void testExtractNumericSignal() {
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("viewCount")
                .normalizedName("norm_views")
                .fieldPath("viewCount")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.sigmoid(1000.0, 1.0, 2.0))
                .boost(0.8)
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("viewCount", 500);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    assertEquals(result.size(), 1);
    SignalValue views = result.get("viewCount");
    assertNotNull(views);
    assertEquals(views.getRawValue(), 500);
    assertEquals(views.getNumericValue(), 500.0);
    assertTrue(views.getNormalizedValue() >= 1.0 && views.getNormalizedValue() <= 2.0);
    assertEquals(views.getBoost(), 0.8);
  }

  @Test
  public void testExtractBooleanSignal() {
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("hasDescription")
                .normalizedName("hasDesc")
                .fieldPath("hasDescription")
                .type(SignalType.BOOLEAN)
                .normalization(NormalizationConfig.bool(1.3, 1.0))
                .boost(1.3)
                .build());

    // Test true value
    Map<String, Object> sourceMapTrue = new HashMap<>();
    sourceMapTrue.put("hasDescription", true);
    SearchHit hitTrue = mock(SearchHit.class);
    when(hitTrue.getId()).thenReturn("doc1");
    when(hitTrue.getScore()).thenReturn(50.0f);
    when(hitTrue.getSourceAsMap()).thenReturn(sourceMapTrue);

    Map<String, SignalValue> resultTrue = extractor.extract(hitTrue, 50.0, signals);
    assertEquals(resultTrue.get("hasDescription").getNormalizedValue(), 1.3);

    // Test false value
    Map<String, Object> sourceMapFalse = new HashMap<>();
    sourceMapFalse.put("hasDescription", false);
    SearchHit hitFalse = mock(SearchHit.class);
    when(hitFalse.getId()).thenReturn("doc2");
    when(hitFalse.getScore()).thenReturn(50.0f);
    when(hitFalse.getSourceAsMap()).thenReturn(sourceMapFalse);

    Map<String, SignalValue> resultFalse = extractor.extract(hitFalse, 50.0, signals);
    assertEquals(resultFalse.get("hasDescription").getNormalizedValue(), 1.0);
  }

  @Test
  public void testExtractNestedPath() {
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("queryCount")
                .normalizedName("norm_queries")
                .fieldPath("statsSummary.queryCount")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.sigmoid(1000.0, 1.0, 1.8))
                .boost(0.8)
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    Map<String, Object> stats = new HashMap<>();
    stats.put("queryCount", 250);
    sourceMap.put("statsSummary", stats);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    assertEquals(result.size(), 1);
    SignalValue queries = result.get("queryCount");
    assertNotNull(queries);
    assertEquals(queries.getRawValue(), 250);
    assertEquals(queries.getNumericValue(), 250.0);
  }

  @Test
  public void testExtractMissingField() {
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("viewCount")
                .normalizedName("norm_views")
                .fieldPath("viewCount")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.sigmoid(1000.0, 1.0, 2.0))
                .boost(0.8)
                .build());

    Map<String, Object> sourceMap = new HashMap<>(); // No viewCount field
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    assertEquals(result.size(), 1);
    SignalValue views = result.get("viewCount");
    assertNotNull(views);
    assertNull(views.getRawValue());
    assertEquals(views.getNumericValue(), 0.0); // Missing fields default to 0
  }

  @Test
  public void testZeroInputMaxReturnsMin() {
    // Test that zero inputMax doesn't cause division by zero
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("viewCount")
                .normalizedName("norm_views")
                .fieldPath("viewCount")
                .type(SignalType.NUMERIC)
                .normalization(
                    NormalizationConfig.builder()
                        .type(NormalizationType.SIGMOID)
                        .inputMin(0.0)
                        .inputMax(0.0) // Zero inputMax - edge case (inputMax <= inputMin)
                        .steepness(6.0)
                        .outputMin(1.0)
                        .outputMax(2.0)
                        .build())
                .boost(1.0)
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("viewCount", 100);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    // Should return outputMin (1.0) when inputMax <= inputMin
    assertEquals(result.get("viewCount").getNormalizedValue(), 1.0);
  }

  @Test
  public void testZeroScaleReturnsMin() {
    // Test that zero scale doesn't cause division by zero
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("lastModified")
                .normalizedName("recency")
                .fieldPath("lastModified")
                .type(SignalType.TIMESTAMP)
                .normalization(
                    NormalizationConfig.builder()
                        .type(NormalizationType.LINEAR_DECAY)
                        .scale(0.0) // Zero scale - edge case
                        .outputMin(0.7)
                        .outputMax(1.2)
                        .build())
                .boost(1.0)
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("lastModified", System.currentTimeMillis() - 86400000L); // 1 day ago
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    // Should return outputMin (0.7) when scale is 0, not NaN or throw exception
    assertEquals(result.get("lastModified").getNormalizedValue(), 0.7);
  }

  @Test
  public void testSteepnessAffectsOutput() {
    // Test that different steepness values produce different outputs
    SignalDefinition signalLowSteepness =
        SignalDefinition.builder()
            .name("viewCount")
            .normalizedName("norm_views")
            .fieldPath("viewCount")
            .type(SignalType.NUMERIC)
            .normalization(NormalizationConfig.sigmoid(0.0, 1000.0, 2.0, 1.0, 2.0)) // Low steepness
            .boost(1.0)
            .build();

    SignalDefinition signalHighSteepness =
        SignalDefinition.builder()
            .name("viewCount")
            .normalizedName("norm_views")
            .fieldPath("viewCount")
            .type(SignalType.NUMERIC)
            .normalization(
                NormalizationConfig.sigmoid(0.0, 1000.0, 10.0, 1.0, 2.0)) // High steepness
            .boost(1.0)
            .build();

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("viewCount", 300); // 30% of range - should show difference
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> resultLow =
        extractor.extract(hit, 50.0, Arrays.asList(signalLowSteepness));
    Map<String, SignalValue> resultHigh =
        extractor.extract(hit, 50.0, Arrays.asList(signalHighSteepness));

    // With low steepness (2.0), output should be closer to middle
    // With high steepness (10.0), output should be closer to outputMin
    double lowSteepnessValue = resultLow.get("viewCount").getNormalizedValue();
    double highSteepnessValue = resultHigh.get("viewCount").getNormalizedValue();

    // Both should be in valid range
    assertTrue(lowSteepnessValue >= 1.0 && lowSteepnessValue <= 2.0);
    assertTrue(highSteepnessValue >= 1.0 && highSteepnessValue <= 2.0);

    // High steepness should be more extreme (further from 1.5) at 30% of range
    // At 30%, high steepness gives lower value (closer to outputMin)
    assertTrue(highSteepnessValue < lowSteepnessValue);
  }

  @Test
  public void testContributionCalculation() {
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("hasDescription")
                .normalizedName("hasDesc")
                .fieldPath("hasDescription")
                .type(SignalType.BOOLEAN)
                .normalization(NormalizationConfig.bool(1.3, 1.0))
                .boost(1.5)
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("hasDescription", true);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    SignalValue hasDesc = result.get("hasDescription");
    // Contribution = pow(normalizedValue, boost) = pow(1.3, 1.5)
    double expected = Math.pow(1.3, 1.5);
    assertEquals(hasDesc.getContribution(), expected, 0.0001);
  }

  @Test
  public void testNullSourceMapHandledGracefully() {
    // Test that null source map doesn't cause NPE
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("viewCount")
                .normalizedName("norm_views")
                .fieldPath("viewCount")
                .type(SignalType.NUMERIC)
                .normalization(NormalizationConfig.sigmoid(1000.0, 1.0, 2.0))
                .boost(1.0)
                .build());

    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(null); // Null source map!

    // Should not throw NPE
    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    assertEquals(result.size(), 1);
    SignalValue views = result.get("viewCount");
    assertNull(views.getRawValue()); // Field not found in null source
    assertEquals(views.getNumericValue(), 0.0);
  }

  @Test
  public void testNegativeNormalizedValueWithFractionalBoostHandled() {
    // Test that negative normalized value with fractional boost doesn't produce NaN
    // This can happen if outputMin is negative (a penalty signal)
    List<SignalDefinition> signals =
        Arrays.asList(
            SignalDefinition.builder()
                .name("penalty")
                .normalizedName("penalty")
                .fieldPath("penaltyField")
                .type(SignalType.NUMERIC)
                .normalization(
                    NormalizationConfig.builder()
                        .type(NormalizationType.NONE) // Pass-through
                        .build())
                .boost(1.5) // Fractional boost
                .build());

    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("penaltyField", -0.5); // Negative value
    SearchHit hit = mock(SearchHit.class);
    when(hit.getId()).thenReturn("doc1");
    when(hit.getScore()).thenReturn(50.0f);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    Map<String, SignalValue> result = extractor.extract(hit, 50.0, signals);

    SignalValue penalty = result.get("penalty");
    // pow(-0.5, 1.5) would be NaN, but should be replaced with 1.0
    assertEquals(penalty.getContribution(), 1.0, 0.001);
    assertFalse(Double.isNaN(penalty.getContribution()));
  }
}
