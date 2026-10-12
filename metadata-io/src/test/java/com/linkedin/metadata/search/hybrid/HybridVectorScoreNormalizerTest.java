package com.linkedin.metadata.search.hybrid;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class HybridVectorScoreNormalizerTest {

  @DataProvider
  public Object[][] normalizedScores() {
    return new Object[][] {
      {SearchEngineType.ELASTICSEARCH_8, "cosine", 0.75d, 0.75d},
      {SearchEngineType.ELASTICSEARCH_9, "cosinesimil", 0.25d, 0.25d},
      // OpenSearch 2.19+ returns cosine scores scaled to [0, 1]: identical vectors score 1.0,
      // not 2.0 (opensearch-project/k-NN#2561)
      {SearchEngineType.OPENSEARCH_3, "cosinesimil", 0.75d, 0.75d},
      {SearchEngineType.OPENSEARCH_3, "cosinesimil", 1.0d, 1.0d},
      {SearchEngineType.ELASTICSEARCH_8, "l2_norm", 0.2d, 0.2d},
      {SearchEngineType.OPENSEARCH_3, "l2", 0.2d, 0.2d},
      {SearchEngineType.ELASTICSEARCH_8, "dot_product", 0.8d, 0.8d},
      {SearchEngineType.OPENSEARCH_3, "innerproduct", 1.6d, 0.8d},
      {SearchEngineType.OPENSEARCH_3, "innerproduct", 2d / 3d, 0.25d}
    };
  }

  @Test(dataProvider = "normalizedScores")
  public void testNormalizesRawEngineScores(
      SearchEngineType engineType, String metric, double rawScore, double expected) {
    HybridVectorScoreNormalizer normalizer = new HybridVectorScoreNormalizer(engineType, metric);

    assertEquals(normalizer.normalize(rawScore), expected, 1e-9);
  }

  @DataProvider
  public Object[][] invalidScores() {
    return new Object[][] {
      {Double.NaN}, {Double.POSITIVE_INFINITY}, {Double.NEGATIVE_INFINITY}, {-0.01d}
    };
  }

  @Test(dataProvider = "invalidScores")
  public void testInvalidScoresContributeZero(double rawScore) {
    HybridVectorScoreNormalizer normalizer =
        new HybridVectorScoreNormalizer(SearchEngineType.OPENSEARCH_3, "cosinesimil");

    assertEquals(normalizer.normalize(rawScore), 0d);
  }

  @Test
  public void testClampsScoresToUnitInterval() {
    assertEquals(
        new HybridVectorScoreNormalizer(SearchEngineType.OPENSEARCH_3, "cosinesimil").normalize(3d),
        1d);
    assertEquals(
        new HybridVectorScoreNormalizer(SearchEngineType.ELASTICSEARCH_8, "l2_norm").normalize(2d),
        1d);
  }

  @Test
  public void testRejectsUnsupportedEngineAndMetric() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HybridVectorScoreNormalizer(SearchEngineType.UNKNOWN, "cosine"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HybridVectorScoreNormalizer(SearchEngineType.ELASTICSEARCH_8, "l1"));
  }
}
