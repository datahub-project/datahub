package com.linkedin.metadata.search.hybrid;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class HybridLexicalScoreNormalizerTest {

  @Test
  public void testDefaultScaleCentersTheInputRange() {
    HybridLexicalScoreNormalizer normalizer = new HybridLexicalScoreNormalizer();

    assertEquals(normalizer.normalize(0d), -3d);
    assertEquals(normalizer.normalize(250d), 0d);
    assertEquals(normalizer.normalize(500d), 3d);
  }

  @Test
  public void testClampsToConfiguredRange() {
    HybridLexicalScoreNormalizer normalizer = new HybridLexicalScoreNormalizer(10d, 60d, 4d);

    assertEquals(normalizer.normalize(-100d), -2d);
    assertEquals(normalizer.normalize(35d), 0d);
    assertEquals(normalizer.normalize(1000d), 2d);
  }

  @Test
  public void testRejectsInvalidConfiguration() {
    assertThrows(
        IllegalArgumentException.class, () -> new HybridLexicalScoreNormalizer(1d, 1d, 6d));
    assertThrows(
        IllegalArgumentException.class, () -> new HybridLexicalScoreNormalizer(0d, 1d, 0d));
  }
}
