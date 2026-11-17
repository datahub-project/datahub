package com.linkedin.metadata.search.hybrid;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import org.testng.annotations.Test;

public class HybridScoreCombinerTest {

  private static final Urn DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:my_document");

  @Test
  public void testCombineUsesWeightedNormalizedVectorAndSigmoidLexicalRelevance() {
    HybridScoreCombiner combiner = new HybridScoreCombiner(0.25d);

    HybridCandidate candidate = combiner.combine(DOCUMENT_URN, 2d, 0.6d);

    double expectedLexical = 1d / (1d + Math.exp(-2d));
    assertEquals(candidate.entity(), DOCUMENT_URN);
    assertEquals(candidate.scaledLexicalScore(), 2d);
    assertEquals(candidate.vectorScore(), 0.6d);
    assertEquals(candidate.normalizedLexicalScore(), expectedLexical, 1e-9);
    assertEquals(candidate.normalizedVectorScore(), 0.6d, 1e-9);
    assertEquals(candidate.combinedScore(), (0.25d * 0.6d) + (0.75d * expectedLexical), 1e-9);
  }

  @Test
  public void testInvalidNormalizedVectorScoreContributesZero() {
    HybridScoreCombiner combiner = new HybridScoreCombiner(0.25d);

    assertEquals(combiner.combine(DOCUMENT_URN, 2d, Double.NaN).normalizedVectorScore(), 0d);
    assertEquals(combiner.combine(DOCUMENT_URN, 2d, -1d).normalizedVectorScore(), 0d);
  }

  @Test
  public void testSigmoidHandlesLargeNegativeScore() {
    assertEquals(HybridScoreCombiner.sigmoid(-1000d), 0d);
  }

  @Test
  public void testRejectsInvalidVectorWeights() {
    assertThrows(IllegalArgumentException.class, () -> new HybridScoreCombiner(-0.1d));
    assertThrows(IllegalArgumentException.class, () -> new HybridScoreCombiner(1.1d));
  }
}
